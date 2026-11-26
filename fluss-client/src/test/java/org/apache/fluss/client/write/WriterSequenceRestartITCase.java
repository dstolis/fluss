/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.write;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.server.log.WriterStateEntry;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * With V3 writes enabled, a writer whose state the servers expired on several buckets at once, as
 * after a long pause of a CDC pipeline, keeps writing to log tables and to primary key tables, and
 * every row is stored once.
 */
class WriterSequenceRestartITCase {

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(WriterStateExpirationITCaseBase.clusterConf())
                    .build();

    private static final String DATABASE = "test_db";
    private static final int BUCKETS = 3;
    private static final int ROWS_BEFORE_EXPIRATION = 30;
    private static final int ROWS_AFTER_EXPIRATION = 30;

    private Connection conn;
    private Admin admin;

    @BeforeAll
    static void enableV3Writes() throws Exception {
        // before the writers of the tests connect, so they are told that V3 is allowed
        FLUSS_CLUSTER_EXTENSION.finalizeClusterFeature(ClusterFeature.LOG_RECORD_BATCH_VERSION, 3);
    }

    @BeforeEach
    void setup() throws Exception {
        conn = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        admin = conn.getAdmin();
        admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
    }

    @AfterEach
    void teardown() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    void testPrimaryKeyTableWriterKeepsWritingAfterWriterStateExpired() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "restart_pk_table");
        List<TableBucket> buckets =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA_PK)
                                .distributedBy(BUCKETS)
                                .build());

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            List<CompletableFuture<?>> writes = new ArrayList<>();
            for (int i = 0; i < ROWS_BEFORE_EXPIRATION; i++) {
                writes.add(writer.upsert(row(i, "v" + i)));
            }
            writer.flush();
            assertAllSucceeded(writes);
            Set<Long> writerIdsBefore = expireWriterStates(buckets);

            // the same writer continues on every bucket, although none of them knows it anymore
            writes.clear();
            int total = ROWS_BEFORE_EXPIRATION + ROWS_AFTER_EXPIRATION;
            for (int i = ROWS_BEFORE_EXPIRATION; i < total; i++) {
                writes.add(writer.upsert(row(i, "v" + i)));
            }
            writer.flush();
            assertAllSucceeded(writes);

            // each key was inserted once: one change log record per key, and every key is there
            assertThat(changeLogRecords(buckets)).isEqualTo(total);
            Lookuper lookuper = table.newLookup().createLookuper();
            for (int i = 0; i < total; i++) {
                InternalRow stored = lookuper.lookup(row(i)).get().getSingletonRow();
                assertThat(stored).isNotNull();
                assertThat(stored.getString(1).toString()).isEqualTo("v" + i);
            }
            // KV batches carry no epoch, so the writer restarted under a new writer id
            assertThat(activeWriterIds(buckets))
                    .hasSize(1)
                    .doesNotContainAnyElementsOf(writerIdsBefore);
        }
    }

    @Test
    void testLogTableWriterKeepsWritingOnAllBucketsAfterWriterStateExpired() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "restart_log_table");
        List<TableBucket> buckets =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA)
                                .distributedBy(BUCKETS, "a")
                                .build());

        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            List<CompletableFuture<?>> writes = new ArrayList<>();
            for (int i = 0; i < ROWS_BEFORE_EXPIRATION; i++) {
                writes.add(writer.append(row(i, "v" + i)));
            }
            writer.flush();
            assertAllSucceeded(writes);
            Set<Long> writerIdsBefore = expireWriterStates(buckets);

            writes.clear();
            int total = ROWS_BEFORE_EXPIRATION + ROWS_AFTER_EXPIRATION;
            for (int i = ROWS_BEFORE_EXPIRATION; i < total; i++) {
                writes.add(writer.append(row(i, "v" + i)));
            }
            writer.flush();
            assertAllSucceeded(writes);

            assertThat(changeLogRecords(buckets)).isEqualTo(total);
            // V3 batches carry an epoch, so the writer kept its writer id and bumped the epoch
            assertThat(activeWriterIds(buckets)).isEqualTo(writerIdsBefore);
            for (TableBucket tb : buckets) {
                for (WriterStateEntry state : leaderWriterStates(tb)) {
                    assertThat(state.writerEpoch()).isEqualTo((short) 1);
                }
            }
        }
    }

    private List<TableBucket> createTable(TablePath tablePath, TableDescriptor descriptor)
            throws Exception {
        admin.createTable(tablePath, descriptor, false).get();
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        List<TableBucket> buckets = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKETS; bucket++) {
            TableBucket tb = new TableBucket(tableId, bucket);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
            buckets.add(tb);
        }
        return buckets;
    }

    private static void assertAllSucceeded(List<CompletableFuture<?>> writes) {
        for (CompletableFuture<?> write : writes) {
            assertThat(write).succeedsWithin(Duration.ofMinutes(1));
        }
    }

    /**
     * Expires the writer state of every bucket on all replicas.
     *
     * @return the writer ids the buckets knew, each bucket of which received rows
     */
    private static Set<Long> expireWriterStates(List<TableBucket> buckets) {
        Set<Long> writerIds = activeWriterIds(buckets);
        for (TableBucket tb : buckets) {
            // the rows spread over all buckets, so each one has a sequence to lose
            assertThat(leaderWriterStates(tb)).isNotEmpty();
            for (TabletServer server : FLUSS_CLUSTER_EXTENSION.getTabletServers()) {
                server.getReplicaManager()
                        .getReplicaOrException(tb)
                        .getLogTablet()
                        .removeExpiredWriter(Long.MAX_VALUE);
            }
            assertThat(leaderWriterStates(tb)).isEmpty();
        }
        return writerIds;
    }

    private static Set<Long> activeWriterIds(List<TableBucket> buckets) {
        Set<Long> writerIds = new HashSet<>();
        for (TableBucket tb : buckets) {
            for (WriterStateEntry state : leaderWriterStates(tb)) {
                writerIds.add(state.writerId());
            }
        }
        return writerIds;
    }

    private static List<WriterStateEntry> leaderWriterStates(TableBucket tb) {
        return new ArrayList<>(
                FLUSS_CLUSTER_EXTENSION
                        .waitAndGetLeaderReplica(tb)
                        .getLogTablet()
                        .activeWriters()
                        .values());
    }

    /** The number of records in the logs of the buckets. */
    private static long changeLogRecords(List<TableBucket> buckets) {
        long records = 0;
        for (TableBucket tb : buckets) {
            records += FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb).getLocalLogEndOffset();
        }
        return records;
    }
}
