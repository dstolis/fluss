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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.KafkaValueConversionType;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.kafka.KafkaSocketTestUtils.connect;
import static org.apache.fluss.kafka.KafkaSocketTestUtils.sendRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises idempotent Kafka Produce on a cluster that allows log record batches of magic V3:
 * producer IDs, duplicate detection by record sequence, gaps, epoch fencing and leader failover.
 */
class KafkaIdempotentProduceITCase {
    private static final String DATABASE = "idempotent_produce";
    private static final byte[] KEY = "key".getBytes(StandardCharsets.UTF_8);

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConfig())
                    .setTabletServerListeners("FLUSS://localhost:0,KAFKA://localhost:0")
                    .build();

    @BeforeAll
    static void allowRecordBatchV3() throws Exception {
        // The only place of this test that knows how the level is raised. A new cluster already
        // starts at the latest level, so this only waits until every tablet server applied it.
        CLUSTER.finalizeClusterFeature(ClusterFeature.LOG_RECORD_BATCH_VERSION, 3);
    }

    @Test
    void testDefaultKafkaProducerWritesEachRecordOnceInOrder() throws Exception {
        TablePath path = createTable("default_producer");
        int count = 2000;
        // No idempotence, acks or in-flight settings: since Kafka 3.0 idempotence is on by default.
        Map<String, Object> config = new HashMap<>();
        ServerNode node = CLUSTER.getTabletServerNodes("KAFKA").get(0);
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, node.host() + ":" + node.port());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 512);
        List<Future<RecordMetadata>> writes = new ArrayList<>();
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
            for (int i = 0; i < count; i++) {
                writes.add(producer.send(new ProducerRecord<>(path.toString(), 0, KEY, value(i))));
            }
            producer.flush();
        }
        for (int i = 0; i < count; i++) {
            assertThat(writes.get(i).get(30, TimeUnit.SECONDS).offset()).isEqualTo(i);
        }
        assertThat(readValues(path, count)).isEqualTo(expectedValues(0, count));
    }

    @Test
    void testRetriedBatchIsStoredOnceWithItsOriginalOffset() throws Exception {
        TablePath path = createTable("retry");
        try (Socket socket = connect(leaderKafkaNode(path), 15000)) {
            long producerId = initProducerId(socket);
            PartitionProduceResponse first = produce(socket, path, producerId, 0, 0, 0, 3);
            assertThat(first.errorCode()).isEqualTo(Errors.NONE.code());

            PartitionProduceResponse retry = produce(socket, path, producerId, 0, 0, 0, 3);
            assertThat(retry.errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(retry.baseOffset()).isEqualTo(first.baseOffset());

            PartitionProduceResponse next = produce(socket, path, producerId, 0, 3, 3, 2);
            assertThat(next.errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(next.baseOffset()).isEqualTo(first.baseOffset() + 3);
        }
        assertThat(readValues(path, 5)).isEqualTo(expectedValues(0, 5));
    }

    @Test
    void testGapInSequenceIsRejected() throws Exception {
        TablePath path = createTable("gap");
        try (Socket socket = connect(leaderKafkaNode(path), 15000)) {
            long producerId = initProducerId(socket);
            assertThat(produce(socket, path, producerId, 0, 0, 0, 3).errorCode())
                    .isEqualTo(Errors.NONE.code());
            // records 3 and 4 are missing
            assertThat(produce(socket, path, producerId, 0, 5, 5, 1).errorCode())
                    .isEqualTo(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER.code());
        }
        assertThat(readValues(path, 3)).isEqualTo(expectedValues(0, 3));
    }

    @Test
    void testNewEpochRestartsSequenceAndFencesTheOldOne() throws Exception {
        TablePath path = createTable("epoch");
        try (Socket socket = connect(leaderKafkaNode(path), 15000)) {
            long producerId = initProducerId(socket);
            assertThat(produce(socket, path, producerId, 0, 0, 0, 2).errorCode())
                    .isEqualTo(Errors.NONE.code());
            // the producer bumps its epoch and counts from 0 again
            assertThat(produce(socket, path, producerId, 1, 0, 2, 2).errorCode())
                    .isEqualTo(Errors.NONE.code());
            // a late batch from epoch 0 must not land after the records of epoch 1
            assertThat(produce(socket, path, producerId, 0, 2, 100, 1).errorCode())
                    .isEqualTo(Errors.INVALID_PRODUCER_EPOCH.code());
            // a new epoch that does not start at 0 is a gap
            assertThat(produce(socket, path, producerId, 2, 5, 100, 1).errorCode())
                    .isEqualTo(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER.code());
        }
        assertThat(readValues(path, 4)).isEqualTo(expectedValues(0, 4));
    }

    @Test
    void testRecordsWithNullValueTakeASequenceNumber() throws Exception {
        TablePath path = createTable("nulls");
        try (Socket socket = connect(leaderKafkaNode(path), 15000)) {
            long producerId = initProducerId(socket);
            PartitionProduceResponse nulls =
                    produce(socket, path, producerId, 0, 0, records(null, null));
            assertThat(nulls.errorCode()).isEqualTo(Errors.NONE.code());
            // the two records are stored as rows, so the next batch continues at sequence 2
            PartitionProduceResponse next = produce(socket, path, producerId, 0, 2, 0, 2);
            assertThat(next.errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(next.baseOffset()).isEqualTo(nulls.baseOffset() + 2);
        }
        List<String> expected = new ArrayList<>();
        expected.add(null);
        expected.add(null);
        expected.addAll(expectedValues(0, 2));
        assertThat(readValues(path, 4)).isEqualTo(expected);
    }

    @Test
    void testRetryAfterLeaderFailoverIsStillDetected() throws Exception {
        TablePath path = createTable("failover");
        TableBucket bucket = bucketOf(path);
        int oldLeader = CLUSTER.waitAndGetLeader(bucket);
        long producerId;
        PartitionProduceResponse first;
        try (Socket socket = connect(kafkaNode(oldLeader), 15000)) {
            producerId = initProducerId(socket);
            // acks=-1, so the batch is on every in-sync replica before it is acknowledged
            first = produce(socket, path, producerId, 0, 0, 0, 3);
            assertThat(first.errorCode()).isEqualTo(Errors.NONE.code());
        }
        CLUSTER.stopTabletServer(oldLeader);
        try {
            int newLeader =
                    waitValue(
                            () -> {
                                int leader = CLUSTER.waitAndGetLeader(bucket);
                                return leader == oldLeader
                                        ? Optional.<Integer>empty()
                                        : Optional.of(leader);
                            },
                            Duration.ofMinutes(1),
                            "No new leader was elected for " + bucket);
            try (Socket socket = connect(kafkaNode(newLeader), 15000)) {
                // the acknowledgement was "lost": the producer retries on the new leader
                PartitionProduceResponse retry = produce(socket, path, producerId, 0, 0, 0, 3);
                assertThat(retry.errorCode()).isEqualTo(Errors.NONE.code());
                assertThat(retry.baseOffset()).isEqualTo(first.baseOffset());
                assertThat(produce(socket, path, producerId, 0, 3, 3, 1).errorCode())
                        .isEqualTo(Errors.NONE.code());
            }
            assertThat(readValues(path, 4)).isEqualTo(expectedValues(0, 4));
        } finally {
            CLUSTER.startTabletServer(oldLeader);
        }
    }

    // ------------------------------------------------------------------------------------------

    private static TablePath createTable(String name) throws Exception {
        TablePath path = TablePath.of(DATABASE, name);
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
            admin.createTable(path, tableDescriptor(), false).get();
        }
        CLUSTER.waitUntilAllGatewayHasSameMetadata();
        CLUSTER.waitUntilAllReplicaReady(bucketOf(path));
        return path;
    }

    static TableDescriptor tableDescriptor() {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column("_kafka_value", DataTypes.STRING()).build())
                .distributedBy(1)
                .logFormat(LogFormat.ARROW)
                .property(ConfigOptions.TABLE_KAFKA_ENABLED, true)
                .property(
                        ConfigOptions.TABLE_KAFKA_CONVERT_VALUE_TYPE,
                        KafkaValueConversionType.STRING)
                .build();
    }

    private static TableBucket bucketOf(TablePath path) throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = connection.getAdmin()) {
            return new TableBucket(admin.getTableInfo(path).get().getTableId(), 0);
        }
    }

    private static ServerNode leaderKafkaNode(TablePath path) throws Exception {
        return kafkaNode(CLUSTER.waitAndGetLeader(bucketOf(path)));
    }

    private static ServerNode kafkaNode(int serverId) {
        for (ServerNode node : CLUSTER.getTabletServerNodes("KAFKA")) {
            if (node.id() == serverId) {
                return node;
            }
        }
        throw new IllegalStateException("No Kafka listener for tablet server " + serverId);
    }

    private static long initProducerId(Socket socket) throws Exception {
        InitProducerIdResponse response =
                (InitProducerIdResponse) sendRequest(socket, initRequest());
        assertThat(response.error()).isEqualTo(Errors.NONE);
        assertThat(response.data().producerEpoch()).isZero();
        return response.data().producerId();
    }

    static InitProducerIdRequest initRequest() {
        return new InitProducerIdRequest.Builder(
                        new InitProducerIdRequestData()
                                .setTransactionalId(null)
                                .setTransactionTimeoutMs(Integer.MAX_VALUE))
                .build((short) 4);
    }

    /** Sends {@code count} records numbered from {@code firstValue}, starting at {@code seq}. */
    private static PartitionProduceResponse produce(
            Socket socket,
            TablePath path,
            long producerId,
            int epoch,
            int baseSequence,
            int firstValue,
            int count)
            throws Exception {
        byte[][] values = new byte[count][];
        for (int i = 0; i < count; i++) {
            values[i] = value(firstValue + i);
        }
        return produce(socket, path, producerId, epoch, baseSequence, records(values));
    }

    static PartitionProduceResponse produce(
            Socket socket,
            TablePath path,
            long producerId,
            int epoch,
            int baseSequence,
            SimpleRecord[] records)
            throws Exception {
        MemoryRecords batch =
                MemoryRecords.withIdempotentRecords(
                        Compression.NONE, producerId, (short) epoch, baseSequence, records);
        List<TopicProduceData> topics = new ArrayList<>();
        topics.add(
                new TopicProduceData()
                        .setName(path.toString())
                        .setPartitionData(
                                Collections.singletonList(
                                        new PartitionProduceData().setIndex(0).setRecords(batch))));
        ProduceRequest request =
                new ProduceRequest(
                        new ProduceRequestData()
                                .setAcks((short) -1)
                                .setTimeoutMs(10000)
                                .setTopicData(
                                        new ProduceRequestData.TopicProduceDataCollection(
                                                topics.iterator())),
                        (short) 9);
        ProduceResponse response = (ProduceResponse) sendRequest(socket, request);
        return response.data().responses().find(path.toString()).partitionResponses().get(0);
    }

    static SimpleRecord[] records(byte[]... values) {
        SimpleRecord[] records = new SimpleRecord[values.length];
        for (int i = 0; i < values.length; i++) {
            records[i] = new SimpleRecord(System.currentTimeMillis(), KEY, values[i]);
        }
        return records;
    }

    /** Reads the values of the first bucket, null for a record without a value. */
    static List<String> readValues(FlussClusterExtension cluster, TablePath path, int expectedCount)
            throws Exception {
        List<String> values = new ArrayList<>();
        try (Connection connection = ConnectionFactory.createConnection(cluster.getClientConfig());
                Table table = connection.getTable(path);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (values.size() < expectedCount && System.nanoTime() < deadline) {
                for (ScanRecord record : scanner.poll(Duration.ofSeconds(1))) {
                    values.add(valueOf(record));
                }
            }
            // a record stored twice would show up as one more row
            for (ScanRecord record : scanner.poll(Duration.ofSeconds(2))) {
                values.add(valueOf(record));
            }
        }
        return values;
    }

    private static List<String> readValues(TablePath path, int expectedCount) throws Exception {
        return readValues(CLUSTER, path, expectedCount);
    }

    private static @Nullable String valueOf(ScanRecord record) {
        return record.getRow().isNullAt(0) ? null : record.getRow().getString(0).toString();
    }

    static List<String> expectedValues(int from, int to) {
        List<String> values = new ArrayList<>();
        for (int i = from; i < to; i++) {
            values.add("v-" + i);
        }
        return values;
    }

    static byte[] value(int i) {
        return ("v-" + i).getBytes(StandardCharsets.UTF_8);
    }

    static Configuration clusterConfig() {
        Configuration config = new Configuration();
        config.set(ConfigOptions.KAFKA_ENABLED, true);
        config.set(ConfigOptions.KAFKA_DEFAULT_DATABASE, DATABASE);
        config.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        config.set(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 2);
        return config;
    }
}
