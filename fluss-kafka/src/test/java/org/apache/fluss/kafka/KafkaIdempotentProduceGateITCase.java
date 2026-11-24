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

package org.apache.fluss.kafka;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.kafka.KafkaIdempotentProduceITCase.expectedValues;
import static org.apache.fluss.kafka.KafkaIdempotentProduceITCase.initRequest;
import static org.apache.fluss.kafka.KafkaIdempotentProduceITCase.produce;
import static org.apache.fluss.kafka.KafkaIdempotentProduceITCase.readValues;
import static org.apache.fluss.kafka.KafkaIdempotentProduceITCase.records;
import static org.apache.fluss.kafka.KafkaIdempotentProduceITCase.tableDescriptor;
import static org.apache.fluss.kafka.KafkaIdempotentProduceITCase.value;
import static org.apache.fluss.kafka.KafkaSocketTestUtils.connect;
import static org.apache.fluss.kafka.KafkaSocketTestUtils.sendRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the cluster feature gate of idempotent Kafka Produce: a cluster below the level that
 * allows log record batches of magic V3 refuses idempotent producers and keeps serving the others,
 * and accepts idempotent producers once the level is raised while the cluster runs.
 */
class KafkaIdempotentProduceGateITCase {
    private static final String DATABASE = "idempotent_produce";
    private static final byte[] KEY = "key".getBytes(StandardCharsets.UTF_8);
    private static final String NOT_ENABLED = "message format version on the broker";

    @RegisterExtension static final FlussClusterExtension CLUSTER = clusterBelowRecordBatchV3();

    /**
     * Builds a cluster whose finalized level does not allow log record batches of magic V3 yet. It
     * is the only place of this test that knows how a cluster starts below the level.
     */
    private static FlussClusterExtension clusterBelowRecordBatchV3() {
        // like a cluster upgraded from a version without cluster features, which starts at the
        // baseline level of every feature; a new cluster would start at the latest levels
        return FlussClusterExtension.builder()
                .setBaselineClusterFeatureLevels(true)
                .setNumOfTabletServers(3)
                .setClusterConf(KafkaIdempotentProduceITCase.clusterConfig())
                .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                .build();
    }

    /** The only place of this test that knows how the level is raised. */
    private static void raiseToRecordBatchV3() throws Exception {
        CLUSTER.finalizeClusterFeature(ClusterFeature.LOG_RECORD_BATCH_VERSION, 3);
    }

    @Test
    void testIdempotentProducersWorkOnceTheLevelIsRaised() throws Exception {
        TablePath path = createTable("gate");
        ServerNode leader = leaderKafkaNode(path);
        List<String> stored = new ArrayList<>();

        try (KafkaProducer<byte[], byte[]> plain = producer(leader, false);
                KafkaProducer<byte[], byte[]> earlyIdempotent = producer(leader, true)) {
            // --- below the level ---------------------------------------------------------------
            // a producer without idempotence works as it does today
            assertThat(send(plain, path, 0)).isZero();
            stored.addAll(expectedValues(0, 1));

            try (Socket socket = connect(leader, 15000)) {
                InitProducerIdResponse init =
                        (InitProducerIdResponse) sendRequest(socket, initRequest());
                assertThat(init.error()).isEqualTo(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT);
                // a producer that skips InitProducerId is stopped at the batch
                PartitionProduceResponse rejected =
                        produce(socket, path, 4711L, 0, 0, records(value(100)));
                assertThat(rejected.errorCode())
                        .isEqualTo(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT.code());
                assertThat(rejected.errorMessage())
                        .contains("log.record-batch.version")
                        .contains("enable.idempotence=false");
            }

            // a Kafka producer with its default settings is idempotent and fails at once
            long start = System.nanoTime();
            assertThatThrownBy(() -> send(earlyIdempotent, path, 100))
                    .hasStackTraceContaining(NOT_ENABLED);
            assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)).isLessThan(30L);

            // --- the level is raised while the cluster and the producers run
            // ----------------------
            raiseToRecordBatchV3();

            // the running producer without idempotence is not affected
            assertThat(send(plain, path, 1)).isEqualTo(1L);
            stored.addAll(expectedValues(1, 2));
            // the producer that failed below the level stays failed, it has to be created again
            assertThatThrownBy(() -> send(earlyIdempotent, path, 100))
                    .hasStackTraceContaining(NOT_ENABLED);
        }

        // --- at the level ----------------------------------------------------------------------
        try (KafkaProducer<byte[], byte[]> idempotent = producer(leader, true)) {
            for (int i = 2; i < 12; i++) {
                assertThat(send(idempotent, path, i)).isEqualTo(i);
            }
            stored.addAll(expectedValues(2, 12));
        }
        try (Socket socket = connect(leader, 15000)) {
            InitProducerIdResponse init =
                    (InitProducerIdResponse) sendRequest(socket, initRequest());
            assertThat(init.error()).isEqualTo(Errors.NONE);
            PartitionProduceResponse written =
                    produce(socket, path, init.data().producerId(), 0, 0, records(value(12)));
            assertThat(written.errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(written.baseOffset()).isEqualTo(12L);
            stored.addAll(expectedValues(12, 13));
        }
        assertThat(readValues(CLUSTER, path, stored.size())).isEqualTo(stored);
    }

    private static long send(KafkaProducer<byte[], byte[]> producer, TablePath path, int i)
            throws Exception {
        return producer.send(new ProducerRecord<>(path.toString(), 0, KEY, value(i)))
                .get(30, TimeUnit.SECONDS)
                .offset();
    }

    private static KafkaProducer<byte[], byte[]> producer(ServerNode node, boolean idempotent) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, node.host() + ":" + node.port());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30000);
        if (!idempotent) {
            // idempotence is on by default since Kafka 3.0
            config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        }
        return new KafkaProducer<>(config);
    }

    private static TablePath createTable(String name) throws Exception {
        TablePath path = TablePath.of(DATABASE, name);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, tableDescriptor(), false).get();
            CLUSTER.waitUntilAllGatewayHasSameMetadata();
            CLUSTER.waitUntilAllReplicaReady(
                    new TableBucket(admin.getTableInfo(path).get().getTableId(), 0));
        }
        return path;
    }

    private static ServerNode leaderKafkaNode(TablePath path) throws Exception {
        int leader;
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            leader =
                    CLUSTER.waitAndGetLeader(
                            new TableBucket(admin.getTableInfo(path).get().getTableId(), 0));
        }
        for (ServerNode node : CLUSTER.getTabletServerNodes("KAFKA")) {
            if (node.id() == leader) {
                return node;
            }
        }
        throw new IllegalStateException("No Kafka listener for tablet server " + leader);
    }
}
