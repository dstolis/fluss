/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.tablet;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordBatchFormat;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.PbFetchLogRespForBucket;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.UpdateClusterFeaturesRequest;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newFetchLogRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsV3;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests enabling log record batches of magic V3 through the cluster feature level. */
class LogRecordBatchV3ITCase {

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConf())
                    // like an upgraded cluster, the tests raise the level themselves
                    .setBaselineClusterFeatureLevels(true)
                    .build();

    private static final long WRITER_ID = 42L;

    @Test
    void testWriteAndReadV3BatchesOnceEnabled() throws Exception {
        alterFeatureLevel("3");
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

        // every tablet server reports the level to the writers that ask for its API versions
        for (int serverId = 0; serverId < 3; serverId++) {
            ApiVersionsResponse versions =
                    FLUSS_CLUSTER_EXTENSION
                            .newTabletServerClientForNode(serverId)
                            .apiVersions(
                                    new ApiVersionsRequest()
                                            .setClientSoftwareName("test")
                                            .setClientSoftwareVersion("test"))
                            .get();
            assertThat(versions.getFinalizedFeaturesList())
                    .singleElement()
                    .satisfies(
                            feature -> {
                                assertThat(feature.getName()).isEqualTo("log.record-batch.version");
                                assertThat(feature.getLevel()).isEqualTo(3);
                            });
        }

        // the level is never lowered, because batches written at level 3 must stay readable
        assertThatThrownBy(() -> alterFeatureLevel("2"))
                .rootCause()
                .hasMessageContaining("Cannot lower cluster feature 'log.record-batch.version'");

        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION,
                        TablePath.of("fluss", "v3_log_table"),
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA)
                                .distributedBy(1)
                                .logFormat(LogFormat.INDEXED)
                                .build());
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
        int leaderId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leader = FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderId);
        List<Object[]> records = DATA1.subList(0, 3);

        // the connection negotiated PRODUCE_LOG v1 before the raise, which cannot carry V3
        assertThat(
                        leader.produceLog(newProduceLogRequest(tableId, 0, -1, v3Batch(records, 0)))
                                .get()
                                .getBucketsRespsList()
                                .get(0)
                                .getErrorCode())
                .isEqualTo(Errors.UNSUPPORTED_VERSION.code());
        // asking for the API versions again negotiates v2, which the servers now advertise
        FLUSS_CLUSTER_EXTENSION.renegotiateApiVersionsWithTabletServers();
        assertProduceLogResponse(
                leader.produceLog(newProduceLogRequest(tableId, 0, -1, v3Batch(records, 0))).get(),
                0,
                0L);
        assertProduceLogResponse(
                leader.produceLog(newProduceLogRequest(tableId, 0, -1, v3Batch(records, 1))).get(),
                0,
                3L);

        // a client that negotiated FETCH_LOG v1 gets the batches as stored
        FetchLogResponse fetched = leader.fetchLog(newFetchLogRequest(-1, tableId, 0, 0L)).get();
        PbFetchLogRespForBucket bucketResp =
                fetched.getTablesRespsList().get(0).getBucketsRespsList().get(0);
        assertThat(bucketResp.hasErrorCode()).isFalse();
        assertV3Batches(MemoryLogRecords.pointToBytes(bucketResp.getRecords()));

        // a client whose FETCH_LOG version can't read V3 gets the batches converted to V0,
        // with the same records and offsets
        LogRecords converted = fetchAsOldClient(leaderId, tb);
        List<LogRecordBatch> convertedBatches = new ArrayList<>();
        converted.batches().forEach(convertedBatches::add);
        assertThat(convertedBatches).hasSize(2);
        for (int i = 0; i < convertedBatches.size(); i++) {
            LogRecordBatch batch = convertedBatches.get(i);
            batch.ensureValid();
            assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V0);
            assertThat(batch.baseLogOffset()).isEqualTo(i * 3L);
            assertThat(batch.lastLogOffset()).isEqualTo(i * 3L + 2);
        }

        // followers replicate the original batches, V3 header included
        retry(
                Duration.ofMinutes(1),
                () -> {
                    for (int serverId = 0; serverId < 3; serverId++) {
                        LogTablet log =
                                FLUSS_CLUSTER_EXTENSION
                                        .getTabletServerById(serverId)
                                        .getReplicaManager()
                                        .getReplicaOrException(tb)
                                        .getLogTablet();
                        assertThat(log.localLogEndOffset()).isEqualTo(6L);
                        assertV3Batches(
                                log.read(0L, Integer.MAX_VALUE, FetchIsolation.LOG_END, true)
                                        .getRecords());
                    }
                });
    }

    private static Configuration clusterConf() {
        Configuration conf = new Configuration();
        // every tablet server hosts a replica, so the test also covers the followers
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }

    @Test
    void testTabletServerBelowTheLevelGetsNoReplicas() throws Exception {
        alterFeatureLevel("3");
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        int oldServerId = 10;
        // an old version registers after V3 was enabled
        zkClient.registerTabletServer(
                oldServerId,
                new TabletServerRegistration(
                        null,
                        Endpoint.fromListenersString("FLUSS://localhost:1"),
                        System.currentTimeMillis(),
                        TabletServerResource.unknown(),
                        Collections.emptyMap()));
        try {
            // new tables are only placed on the servers that support the level
            long tableId =
                    createTable(
                            FLUSS_CLUSTER_EXTENSION,
                            TablePath.of("fluss", "v3_placement_table"),
                            TableDescriptor.builder()
                                    .schema(DATA1_SCHEMA)
                                    .distributedBy(12)
                                    .build());
            TableAssignment assignment = zkClient.getTableAssignment(tableId).get();
            assertThat(assignment.getBucketAssignments().values())
                    .allSatisfy(
                            bucket -> assertThat(bucket.getReplicas()).doesNotContain(oldServerId));
            // and the server is not part of the cluster metadata
            assertThat(
                            FLUSS_CLUSTER_EXTENSION
                                    .getCoordinatorServer()
                                    .getCoordinatorEventProcessor()
                                    .getCoordinatorContext()
                                    .liveTabletServerSet())
                    .doesNotContain(oldServerId);
        } finally {
            zkClient.deletePath(ZkData.ServerIdZNode.path(oldServerId));
        }
    }

    private static ProduceLogResponse produce(
            TabletServerGateway leader,
            long tableId,
            List<Object[]> records,
            short writerEpoch,
            int baseSequence)
            throws Exception {
        return leader.produceLog(
                        newProduceLogRequest(
                                tableId,
                                0,
                                -1,
                                genMemoryLogRecordsV3(
                                        records, WRITER_ID, writerEpoch, baseSequence, true, 0L)))
                .get();
    }

    private static void assertError(ProduceLogResponse response, Errors error) {
        assertThat(response.getBucketsRespsCount()).isEqualTo(1);
        PbProduceLogRespForBucket bucketResp = response.getBucketsRespsList().get(0);
        assertThat(bucketResp.getErrorCode()).isEqualTo(error.code());
    }

    private static LogRecords fetchAsOldClient(int leaderId, TableBucket tb) throws Exception {
        FetchParams params = new FetchParams(-1, Integer.MAX_VALUE);
        params.setReaderMaxBatchMagic(LogRecordBatchFormat.LOG_MAGIC_VALUE_V2);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> result =
                new CompletableFuture<>();
        FLUSS_CLUSTER_EXTENSION
                .getTabletServerById(leaderId)
                .getReplicaManager()
                .fetchLogRecords(
                        params,
                        Collections.singletonMap(
                                tb, new FetchReqInfo(tb.getTableId(), 0L, Integer.MAX_VALUE)),
                        null,
                        result::complete);
        FetchLogResultForBucket bucketResult = result.get().get(tb);
        assertThat(bucketResult.failed()).isFalse();
        return bucketResult.records();
    }

    private static MemoryLogRecords v3Batch(List<Object[]> records, int batchSequence)
            throws Exception {
        return genMemoryLogRecordsV3(records, WRITER_ID, (short) 0, batchSequence, false, 0L);
    }

    private static void assertV3Batches(LogRecords records) {
        List<LogRecordBatch> batches = new ArrayList<>();
        records.batches().forEach(batches::add);
        assertThat(batches).hasSize(2);
        for (int i = 0; i < batches.size(); i++) {
            LogRecordBatch batch = batches.get(i);
            assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V3);
            assertThat(batch.writerId()).isEqualTo(WRITER_ID);
            assertThat(batch.writerEpoch()).isEqualTo((short) 0);
            assertThat(batch.batchSequence()).isEqualTo(i);
            assertThat(batch.lastSequence()).isEqualTo(i);
        }
    }

    private static void alterFeatureLevel(String level) throws Exception {
        UpdateClusterFeaturesRequest request = new UpdateClusterFeaturesRequest();
        request.addFeature()
                .setName(ClusterFeature.LOG_RECORD_BATCH_VERSION.featureName())
                .setLevel(Integer.parseInt(level));
        FLUSS_CLUSTER_EXTENSION.newCoordinatorClient().updateClusterFeatures(request).get();
    }
}
