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

package org.apache.fluss.server.tablet;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordBatchFormat;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.AlterClusterConfigsRequest;
import org.apache.fluss.rpc.messages.PbAlterConfig;
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsV3;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a client whose {@code FETCH_LOG} version can't read log record batches of magic V3
 * reads them from log segments that were tiered to remote storage.
 */
class LogRecordBatchV3RemoteReadITCase {

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(clusterConf())
                    .build();

    private static final long WRITER_ID = 42L;
    private static final List<Object[]> RECORDS = DATA1.subList(0, 3);
    private static final int BATCHES = 40;

    @Test
    void testOldReaderAdvancesThroughRemoteV3Segments() throws Exception {
        enableV3Writes();
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        TablePath.of("fluss", "v3_remote_read_table"),
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA)
                                .distributedBy(1)
                                .logFormat(LogFormat.INDEXED)
                                // segments are removed locally once they are in remote storage
                                .property(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS, 1)
                                .build());
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
        int leaderId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leader = FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderId);

        int batchSizeInBytes = 0;
        for (int i = 0; i < BATCHES; i++) {
            MemoryLogRecords batch =
                    genMemoryLogRecordsV3(RECORDS, WRITER_ID, (short) 0, i, false, 0L);
            batchSizeInBytes = batch.sizeInBytes();
            assertProduceLogResponse(
                    leader.produceLog(newProduceLogRequest(tableId, 0, -1, batch)).get(),
                    0,
                    (long) i * RECORDS.size());
        }
        long logEndOffset = (long) BATCHES * RECORDS.size();

        // the first batches are only in remote storage
        FLUSS_CLUSTER_EXTENSION.waitUntilSomeLogSegmentsCopyToRemote(tb);
        LogTablet log =
                FLUSS_CLUSTER_EXTENSION
                        .getTabletServerById(leaderId)
                        .getReplicaManager()
                        .getReplicaOrException(tb)
                        .getLogTablet();
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(log.localLogStartOffset())
                                .isGreaterThanOrEqualTo(2L * RECORDS.size()));

        // The reader's fetch size holds one batch, as with the defaults, where a writer batch
        // (2mb) is larger than the fetch size of a bucket (1mb). Every fetch must start with the
        // batch at the fetch offset, otherwise the reader never advances.
        long fetchOffset = 0L;
        int records = 0;
        while (fetchOffset < logEndOffset) {
            List<LogRecordBatch> batches =
                    fetchAsOldClient(leaderId, tb, fetchOffset, batchSizeInBytes);
            assertThat(batches).as("batches fetched at offset %s", fetchOffset).isNotEmpty();
            assertThat(batches.get(0).baseLogOffset())
                    .as("base offset of the first batch fetched at offset %s", fetchOffset)
                    .isEqualTo(fetchOffset);
            for (LogRecordBatch batch : batches) {
                batch.ensureValid();
                assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V0);
                records += batch.getRecordCount();
                fetchOffset = batch.nextLogOffset();
            }
        }
        assertThat(records).isEqualTo(BATCHES * RECORDS.size());

        // a fetch offset inside a batch gets that batch, as for local segments
        long secondBatchBaseOffset = RECORDS.size();
        assertThat(
                        fetchAsOldClient(leaderId, tb, secondBatchBaseOffset + 1, batchSizeInBytes)
                                .get(0)
                                .baseLogOffset())
                .isEqualTo(secondBatchBaseOffset);
    }

    private static Configuration clusterConf() {
        Configuration conf = new Configuration();
        // small segments that are copied to remote storage quickly
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        return conf;
    }

    /** Fetches like a client whose {@code FETCH_LOG} version reads batches up to magic V2. */
    private static List<LogRecordBatch> fetchAsOldClient(
            int leaderId, TableBucket tb, long fetchOffset, int maxBytes) throws Exception {
        FetchParams params = new FetchParams(-1, Integer.MAX_VALUE);
        params.convertBatchesAbove(LogRecordBatchFormat.LOG_MAGIC_VALUE_V2);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> result =
                new CompletableFuture<>();
        FLUSS_CLUSTER_EXTENSION
                .getTabletServerById(leaderId)
                .getReplicaManager()
                .fetchLogRecords(
                        params,
                        Collections.singletonMap(
                                tb, new FetchReqInfo(tb.getTableId(), fetchOffset, maxBytes)),
                        null,
                        result::complete);
        FetchLogResultForBucket bucketResult = result.get().get(tb);
        assertThat(bucketResult.failed()).isFalse();
        // an old reader gets the batches inline, never a remote segment it can't decode
        assertThat(bucketResult.records()).isNotNull();
        List<LogRecordBatch> batches = new ArrayList<>();
        bucketResult.records().batches().forEach(batches::add);
        return batches;
    }

    /** Raises the cluster to the level that allows V3 batches and reconnects to negotiate it. */
    private static void enableV3Writes() throws Exception {
        FLUSS_CLUSTER_EXTENSION
                .newCoordinatorClient()
                .alterClusterConfigs(
                        new AlterClusterConfigsRequest()
                                .addAllAlterConfigs(
                                        Collections.singletonList(
                                                new PbAlterConfig()
                                                        .setConfigKey(
                                                                ConfigOptions
                                                                        .CLUSTER_FEATURE_LOG_RECORD_BATCH_VERSION
                                                                        .key())
                                                        .setConfigValue("3")
                                                        .setOpType(AlterConfigOpType.SET.value()))))
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
        // versions are negotiated once per connection
        for (ServerNode node : FLUSS_CLUSTER_EXTENSION.getTabletServerNodes()) {
            FLUSS_CLUSTER_EXTENSION.getRpcClient().disconnect(node).get();
        }
    }
}
