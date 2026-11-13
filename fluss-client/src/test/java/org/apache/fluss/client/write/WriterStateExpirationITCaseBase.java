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
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.log.WriterStateEntry;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Writes to a log table, lets every replica expire the writer state, as after a long pause of a CDC
 * pipeline, and then writes again with the same writer.
 */
abstract class WriterStateExpirationITCaseBase {

    protected static final int ROWS_BEFORE_EXPIRATION = 5;
    protected static final int ROWS_AFTER_EXPIRATION = 5;

    private static final TablePath TABLE_PATH =
            TablePath.of("test_db", "writer_state_expiration_table");

    protected Connection conn;
    protected TableBucket tableBucket;

    protected abstract FlussClusterExtension cluster();

    protected static Configuration clusterConf() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }

    @BeforeEach
    void setup() throws Exception {
        conn = ConnectionFactory.createConnection(cluster().getClientConfig());
        Admin admin = conn.getAdmin();
        admin.createDatabase(TABLE_PATH.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(
                        TABLE_PATH,
                        TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1).build(),
                        true)
                .get();
        long tableId = admin.getTableInfo(TABLE_PATH).get().getTableId();
        tableBucket = new TableBucket(tableId, 0);
        cluster().waitUntilAllReplicaReady(tableBucket);
    }

    @AfterEach
    void teardown() throws Exception {
        if (conn != null) {
            conn.getAdmin().dropTable(TABLE_PATH, true).get();
            conn.close();
        }
    }

    /**
     * Writes rows one by one, expires the writer state on all replicas and writes more rows with
     * the same writer.
     *
     * @return the failure of the first write after the expiration, or null if all writes succeed
     */
    @Nullable
    protected Throwable writeAcrossWriterStateExpiration() throws Exception {
        try (Table table = conn.getTable(TABLE_PATH)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < ROWS_BEFORE_EXPIRATION; i++) {
                writer.append(row(i, "v" + i)).get();
            }

            for (TabletServer server : cluster().getTabletServers()) {
                server.getReplicaManager()
                        .getReplicaOrException(tableBucket)
                        .getLogTablet()
                        .removeExpiredWriter(Long.MAX_VALUE);
            }
            assertThat(writerStates()).isEmpty();

            for (int i = ROWS_BEFORE_EXPIRATION;
                    i < ROWS_BEFORE_EXPIRATION + ROWS_AFTER_EXPIRATION;
                    i++) {
                try {
                    writer.append(row(i, "v" + i)).get();
                } catch (ExecutionException e) {
                    return e.getCause();
                }
            }
            return null;
        }
    }

    /** The writer states of all replicas of the bucket. */
    protected List<WriterStateEntry> writerStates() {
        List<WriterStateEntry> states = new ArrayList<>();
        for (TabletServer server : cluster().getTabletServers()) {
            Collection<WriterStateEntry> entries =
                    server.getReplicaManager()
                            .getReplicaOrException(tableBucket)
                            .getLogTablet()
                            .activeWriters()
                            .values();
            states.addAll(entries);
        }
        return states;
    }

    /** Reads the first column of every row of the bucket, in log order. */
    protected List<Integer> readAllRows(int expectedCount) throws Exception {
        List<Integer> values = new ArrayList<>();
        try (Table table = conn.getTable(TABLE_PATH);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(tableBucket.getBucket());
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        ScanRecords records = scanner.poll(Duration.ofMillis(200));
                        for (ScanRecord record : records) {
                            values.add(record.getRow().getInt(0));
                        }
                        assertThat(values).hasSizeGreaterThanOrEqualTo(expectedCount);
                    });
        }
        return values;
    }
}
