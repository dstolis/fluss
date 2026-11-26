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

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.client.metrics.TestingWriterMetricGroup;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.entity.ProduceLogDataForBucket;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeProduceLogResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toProduceLogDataForBuckets;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for {@link Sender} tests: builds a {@link Sender} and {@link RecordAccumulator} on a
 * {@link TestingMetadataUpdater} and drives the requests through {@link TestTabletServerGateway}.
 */
abstract class SenderTestBase {
    static final int TOTAL_MEMORY_SIZE = 1024 * 1024;
    static final int MAX_REQUEST_SIZE = 1024 * 1024;
    static final int BATCH_SIZE = 16 * 1024;
    static final int PAGE_SIZE = 256;
    static final int REQUEST_TIMEOUT = 5000;
    static final short ACKS_ALL = -1;
    static final int MAX_INFLIGHT_REQUEST_PER_BUCKET = 5;

    final TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 0);
    TestingMetadataUpdater metadataUpdater;
    RecordAccumulator accumulator = null;
    TestingBucketAssigner bucketAssigner;
    TestingWriterMetricGroup writerMetricGroup;
    Clock clock = SystemClock.getInstance();

    @BeforeEach
    void setupMetadataAndMetrics() {
        Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        tableInfos.put(DATA1_TABLE_PATH, DATA1_TABLE_INFO);
        tableInfos.put(DATA1_TABLE_PATH_PK, DATA1_TABLE_INFO_PK);
        metadataUpdater = new TestingMetadataUpdater(tableInfos);
        writerMetricGroup = TestingWriterMetricGroup.newInstance();
    }

    Sender setupWithIdempotenceState() {
        return setupWithIdempotenceState(createIdempotenceManager(false));
    }

    Sender setupWithIdempotenceState(IdempotenceManager idempotenceManager) {
        return setupWithIdempotenceState(idempotenceManager, Integer.MAX_VALUE, 0);
    }

    Sender setupWithIdempotenceState(
            IdempotenceManager idempotenceManager, int reties, int batchTimeoutMs) {
        return setupWithIdempotenceState(idempotenceManager, reties, batchTimeoutMs, Duration.ZERO);
    }

    Sender setupWithIdempotenceState(
            IdempotenceManager idempotenceManager,
            int reties,
            int batchTimeoutMs,
            Duration retryBackoff) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, new MemorySize(TOTAL_MEMORY_SIZE));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, new MemorySize(BATCH_SIZE));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, new MemorySize(PAGE_SIZE));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT, Duration.ofMillis(batchTimeoutMs));
        conf.set(ConfigOptions.CLIENT_WRITER_RETRY_BACKOFF, retryBackoff);
        conf.set(ConfigOptions.CLIENT_WRITER_RETRY_BACKOFF_MAX, retryBackoff);
        bucketAssigner = new TestingBucketAssigner();
        accumulator =
                new RecordAccumulator(
                        conf,
                        idempotenceManager,
                        writerMetricGroup,
                        clock,
                        new ExponentialBackoff(1000, 2, 10000, 0),
                        (tableInfo, path) -> bucketAssigner);
        return new Sender(
                accumulator,
                REQUEST_TIMEOUT,
                MAX_REQUEST_SIZE,
                ACKS_ALL,
                reties,
                metadataUpdater,
                idempotenceManager,
                writerMetricGroup);
    }

    IdempotenceManager createIdempotenceManager(boolean idempotenceEnabled) {
        return new IdempotenceManager(
                idempotenceEnabled,
                MAX_INFLIGHT_REQUEST_PER_BUCKET,
                metadataUpdater.newRandomTabletServerClient(),
                metadataUpdater);
    }

    void appendToAccumulator(TableBucket tb, GenericRow row, WriteCallback writeCallback)
            throws Exception {
        appendToAccumulator(DATA1_TABLE_INFO, tb, row, writeCallback);
    }

    void appendToAccumulator(
            TableInfo tableInfo, TableBucket tb, GenericRow row, WriteCallback writeCallback)
            throws Exception {
        bucketAssigner.setBucketId(tb.getBucket());
        accumulator.append(
                WriteRecord.forArrowAppend(
                        tableInfo, PhysicalTablePath.of(tableInfo.getTablePath()), row, null),
                writeCallback,
                metadataUpdater.getCluster());
    }

    /** Returns the gateway of the leader of the given bucket of {@code DATA1_TABLE_PATH}. */
    TestTabletServerGateway leaderGateway(TableBucket tb) {
        return (TestTabletServerGateway)
                metadataUpdater.newTabletServerClientForNode(
                        metadataUpdater.leaderFor(DATA1_TABLE_PATH, tb));
    }

    ApiMessage getRequest(TableBucket tb, int index) {
        return leaderGateway(tb).getRequest(index);
    }

    int pendingRequestSize(TableBucket tb) {
        return leaderGateway(tb).pendingRequestSize();
    }

    void failRequest(TableBucket referenceBucket, int index, Throwable throwable) {
        leaderGateway(referenceBucket).failRequest(index, throwable);
    }

    /**
     * Checks that the pending request at {@code index} carries an idempotent batch of the given
     * sequence for {@code tb}, then completes it with {@code response}.
     */
    void finishIdempotentProduceLogRequest(
            int batchSequence, TableBucket tb, int index, ProduceLogResponse response) {
        ApiMessage request = getRequest(tb, index);
        assertThat(request).isInstanceOf(ProduceLogRequest.class);
        assertThat(hasIdempotentRecords(tb, (ProduceLogRequest) request)).isTrue();
        assertBatchSequenceEquals(tb, (ProduceLogRequest) request, batchSequence);
        leaderGateway(tb).response(index, response);
    }

    static ProduceLogResponse createProduceLogResponse(
            TableBucket tb, long baseOffset, long endOffset) {
        return makeProduceLogResponse(
                Collections.singletonList(
                        new ProduceLogResultForBucket(tb, baseOffset, endOffset)));
    }

    static ProduceLogResponse createProduceLogResponse(TableBucket tb, Errors error) {
        return makeProduceLogResponse(
                Collections.singletonList(new ProduceLogResultForBucket(tb, error.toApiError())));
    }

    static boolean hasIdempotentRecords(TableBucket tb, ProduceLogRequest request) {
        MemoryLogRecords memoryLogRecords = getProduceLogRecords(request, tb);
        return memoryLogRecords.batchIterator().next().writerId() != NO_WRITER_ID;
    }

    static void assertBatchSequenceEquals(
            TableBucket tb, ProduceLogRequest request, int expectedBatchSequence) {
        MemoryLogRecords memoryLogRecords = getProduceLogRecords(request, tb);
        assertThat(memoryLogRecords.batchIterator().next().batchSequence())
                .isEqualTo(expectedBatchSequence);
    }

    static MemoryLogRecords getProduceLogRecords(
            ProduceLogRequest request, TableBucket tableBucket) {
        for (ProduceLogDataForBucket bucketData : toProduceLogDataForBuckets(request)) {
            if (bucketData.tableBucket().equals(tableBucket)) {
                return bucketData.records();
            }
        }
        throw new IllegalArgumentException("No records found for table bucket " + tableBucket);
    }
}
