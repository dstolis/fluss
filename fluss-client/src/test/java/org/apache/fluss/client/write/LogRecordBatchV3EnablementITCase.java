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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Enables log record batches of magic V3 while a writer is running. Neither enabling them nor a
 * tablet server restart afterwards may break the writer.
 */
class LogRecordBatchV3EnablementITCase {

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "v3_enablement_table");
    private static final int BUCKETS = 3;
    private static final int ROWS_PER_ROUND = 30;

    @Test
    void testRunningWriterSurvivesEnablingV3AndTabletServerRestart() throws Exception {
        try (Connection conn =
                ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig())) {
            Admin admin = conn.getAdmin();
            admin.createDatabase(TABLE_PATH.getDatabaseName(), DatabaseDescriptor.EMPTY, true)
                    .get();
            admin.createTable(
                            TABLE_PATH,
                            TableDescriptor.builder()
                                    .schema(DATA1_SCHEMA)
                                    .distributedBy(BUCKETS, "a")
                                    // one replica, so a restarted server leads its bucket again
                                    .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1)
                                    .build(),
                            false)
                    .get();
            long tableId = admin.getTableInfo(TABLE_PATH).get().getTableId();
            List<TableBucket> buckets = new ArrayList<>();
            Set<Integer> leaders = new HashSet<>();
            for (int bucket = 0; bucket < BUCKETS; bucket++) {
                TableBucket tb = new TableBucket(tableId, bucket);
                FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
                buckets.add(tb);
                leaders.add(FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb));
            }
            // the writer needs connections to more than one tablet server
            assertThat(leaders).hasSizeGreaterThan(1);

            try (Table table = conn.getTable(TABLE_PATH)) {
                AppendWriter writer = table.newAppend().createWriter();
                // connects the writer to every leader while only the older formats are allowed
                writeToAllBuckets(writer, 0);
                for (TableBucket tb : buckets) {
                    assertThat(
                                    FLUSS_CLUSTER_EXTENSION
                                            .waitAndGetLeaderReplica(tb)
                                            .getLocalLogEndOffset())
                            .isPositive();
                }

                enableV3Writes(admin);
                writeToAllBuckets(writer, 1);

                // The writer reconnects to the restarted leader and keeps its connections to the
                // other leaders, as during a rolling restart.
                int restartedServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(buckets.get(0));
                FLUSS_CLUSTER_EXTENSION.restartTabletServer(restartedServer, new Configuration());
                for (TableBucket tb : buckets) {
                    FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
                }

                for (int round = 2; round < 6; round++) {
                    writeToAllBuckets(writer, round);
                }
            }
        }
    }

    /** Writes rows that spread over all buckets and waits until every row is acknowledged. */
    private static void writeToAllBuckets(AppendWriter writer, int round) {
        List<CompletableFuture<AppendResult>> results = new ArrayList<>();
        for (int i = 0; i < ROWS_PER_ROUND; i++) {
            results.add(writer.append(row(i, "round-" + round)));
        }
        writer.flush();
        for (CompletableFuture<AppendResult> result : results) {
            assertThat(result)
                    .as("a write of round %s", round)
                    .succeedsWithin(Duration.ofMinutes(1));
        }
    }

    private static void enableV3Writes(Admin admin) throws Exception {
        admin.alterClusterConfigs(
                        Collections.singletonList(
                                new AlterConfig(
                                        ConfigOptions.CLUSTER_FEATURE_LOG_RECORD_BATCH_VERSION
                                                .key(),
                                        "3",
                                        AlterConfigOpType.SET)))
                .get();
        // tablet servers apply cluster configs asynchronously
        retry(
                Duration.ofMinutes(1),
                () ->
                        FLUSS_CLUSTER_EXTENSION
                                .getTabletServers()
                                .forEach(
                                        server ->
                                                assertThat(
                                                                server.getReplicaManager()
                                                                        .getClusterFeatures()
                                                                        .levels()
                                                                        .level(
                                                                                ClusterFeature
                                                                                        .LOG_RECORD_BATCH_VERSION))
                                                        .isEqualTo(3)));
    }
}
