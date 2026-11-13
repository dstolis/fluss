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
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecordBatch;
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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeProduceLogResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toProduceLogDataForBuckets;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the writer epoch of the {@link Sender} for log record batches of magic V3. */
final class SenderWriterEpochTest {
    private static final int TOTAL_MEMORY_SIZE = 1024 * 1024;
    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final int BATCH_SIZE = 16 * 1024;
    private static final int PAGE_SIZE = 256;
    private static final int REQUEST_TIMEOUT = 5000;
    private static final short ACKS_ALL = -1;
    private static final int MAX_INFLIGHT_REQUEST_PER_BUCKET = 5;

    private final TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 0);
    private final TableBucket tb2 = new TableBucket(DATA1_TABLE_ID, 1);
    private TestingMetadataUpdater metadataUpdater;
    private RecordAccumulator accumulator;
    private TestingBucketAssigner bucketAssigner;
    private TestingWriterMetricGroup writerMetricGroup;
    private final Clock clock = SystemClock.getInstance();

    @BeforeEach
    void setup() {
        metadataUpdater = initializeMetadataUpdater();
        writerMetricGroup = TestingWriterMetricGroup.newInstance();
    }

    /**
     * Batches of magic V3 carry a writer epoch. When the server reports a real gap, for example
     * because it expired the writer state, the writer keeps its id, bumps the epoch and retries the
     * batch from sequence 0, instead of failing the batch and requesting a new writer id.
     */
    @Test
    void testOutOfOrderSequenceBumpsWriterEpochForV3Batches() throws Exception {
        metadataUpdater.setNegotiatedProduceLogVersion((short) 2);
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        long writerId = idempotenceManager.writerId();

        CompletableFuture<Exception> future0 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future0.complete(e));
        sender1.runOnce();
        assertWriterEpochEquals(tb1, (ProduceLogRequest) getRequest(tb1, 0), 0);
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce();
        assertThat(future0.get()).isNull();

        // The server lost the writer state, so batch 1 is out of order.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(2, "b"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        finishIdempotentProduceLogRequest(
                1, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        assertThat(future1).isNotDone();

        // The next round bumps the epoch and resends the batch from sequence 0.
        sender1.runOnce();
        assertThat(idempotenceManager.writerId()).isEqualTo(writerId);
        assertThat(idempotenceManager.writerEpoch()).isEqualTo((short) 1);
        ProduceLogRequest retried = (ProduceLogRequest) getRequest(tb1, 0);
        assertWriterEpochEquals(tb1, retried, 1);
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 1L, 2L));
        sender1.runOnce();
        assertThat(future1.get()).isNull();

        // Later batches continue under the new epoch.
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(3, "c"), (tb, leo, e) -> future2.complete(e));
        sender1.runOnce();
        assertWriterEpochEquals(tb1, (ProduceLogRequest) getRequest(tb1, 0), 1);
        finishIdempotentProduceLogRequest(1, tb1, 0, createProduceLogResponse(tb1, 2L, 3L));
        sender1.runOnce();
        assertThat(future2.get()).isNull();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
    }

    /**
     * On an epoch bump, the batches still in flight for the bucket are renumbered from sequence 0
     * under the new epoch and resent in order.
     */
    @Test
    void testEpochBumpRenumbersInFlightBatches() throws Exception {
        metadataUpdater.setNegotiatedProduceLogVersion((short) 2);
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();

        CompletableFuture<Exception> future0 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future0.complete(e));
        sender1.runOnce();
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(2, "b"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        assertThat(pendingRequestSize(tb1)).isEqualTo(2);

        // Batch 0 hits a real gap, batch 1 fails because batch 0 is missing.
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        finishIdempotentProduceLogRequest(
                1, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        assertThat(future0).isNotDone();
        assertThat(future1).isNotDone();

        // Both are renumbered under epoch 1 and resent one after the other.
        sender1.runOnce();
        assertThat(idempotenceManager.writerEpoch()).isEqualTo((short) 1);
        assertThat(pendingRequestSize(tb1)).isEqualTo(1);
        assertWriterEpochEquals(tb1, (ProduceLogRequest) getRequest(tb1, 0), 1);
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce();
        assertThat(future0.get()).isNull();
        assertWriterEpochEquals(tb1, (ProduceLogRequest) getRequest(tb1, 0), 1);
        finishIdempotentProduceLogRequest(1, tb1, 0, createProduceLogResponse(tb1, 1L, 2L));
        sender1.runOnce();
        assertThat(future1.get()).isNull();
        // A single bump, although both batches were rejected.
        assertThat(idempotenceManager.writerEpoch()).isEqualTo((short) 1);
    }

    /**
     * The epoch bump waits until every batch in flight for the bucket has been answered, since a
     * batch still waiting for its answer may have been written.
     */
    @Test
    void testEpochBumpWaitsForAllResponses() throws Exception {
        metadataUpdater.setNegotiatedProduceLogVersion((short) 2);
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();

        CompletableFuture<Exception> future0 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future0.complete(e));
        sender1.runOnce();
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(2, "b"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        assertThat(pendingRequestSize(tb1)).isEqualTo(2);

        // batch 0 is rejected, batch 1 is still waiting for its answer
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender1.runOnce();
        assertThat(idempotenceManager.writerEpoch()).isEqualTo((short) 0);

        // once batch 1 is rejected too, both are renumbered under the new epoch
        finishIdempotentProduceLogRequest(
                1, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender1.runOnce();
        assertThat(idempotenceManager.writerEpoch()).isEqualTo((short) 1);
        assertWriterEpochEquals(tb1, (ProduceLogRequest) getRequest(tb1, 0), 1);
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce();
        finishIdempotentProduceLogRequest(1, tb1, 0, createProduceLogResponse(tb1, 1L, 2L));
        sender1.runOnce();
        assertThat(future0.get()).isNull();
        assertThat(future1.get()).isNull();
    }

    /**
     * A batch whose request got no answer may have been written. If the server then reports a gap
     * for it, it is not renumbered under a new epoch, which could write it twice: the batch fails
     * and the writer id is reset, as for other unknown outcomes.
     */
    @Test
    void testUnknownOutcomeResetsWriterIdInsteadOfBumpingEpoch() throws Exception {
        metadataUpdater.setNegotiatedProduceLogVersion((short) 2);
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        long writerId = idempotenceManager.writerId();

        CompletableFuture<Exception> future0 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future0.complete(e));
        sender1.runOnce();
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce();
        assertThat(future0.get()).isNull();

        // the request of batch 1 times out, so it may or may not have been written
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(2, "b"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        failRequest(tb1, 0, new TimeoutException("request timed out"));
        sender1.runOnce();

        // the retry finds the writer state expired on the server
        assertWriterEpochEquals(tb1, (ProduceLogRequest) getRequest(tb1, 0), 0);
        finishIdempotentProduceLogRequest(
                1, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        assertThat(future1.get()).isInstanceOf(OutOfOrderSequenceException.class);
        assertThat(idempotenceManager.isWriterIdValid()).isFalse();
        assertThat(idempotenceManager.writerEpoch()).isEqualTo((short) 0);

        // the writer starts over with a new writer id at epoch 0
        sender1.runOnce();
        sender1.runOnce();
        assertThat(idempotenceManager.writerId()).isNotEqualTo(writerId);
        assertThat(idempotenceManager.writerEpoch()).isEqualTo((short) 0);
    }

    /** Without V3, batches carry no epoch, so a real gap still fails the batch and resets. */
    @Test
    void testOutOfOrderSequenceResetsWriterWithoutV3() throws Exception {
        metadataUpdater.setNegotiatedProduceLogVersion((short) 1);
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();

        CompletableFuture<Exception> future0 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future0.complete(e));
        sender1.runOnce();
        ProduceLogRequest request = (ProduceLogRequest) getRequest(tb1, 0);
        assertThat(getProduceLogRecords(request, tb1).batchIterator().next().magic())
                .isLessThan(LOG_MAGIC_VALUE_V3);
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce();

        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(2, "b"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        finishIdempotentProduceLogRequest(
                1, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        assertThat(future1.get()).isInstanceOf(OutOfOrderSequenceException.class);
        assertThat(idempotenceManager.isWriterIdValid()).isFalse();
    }

    /**
     * A writer writes V3 once one tablet server reports that the cluster allows it, also to the
     * leaders it connected to before. A leader that has not applied the level yet rejects the batch
     * before writing it, so the batch is resent as it is until the leader accepts it.
     */
    @Test
    void testV3BatchRejectedBeforeLeaderAppliedTheLevelIsRetried() throws Exception {
        // only the leader of bucket 0 reported the level, the connection to the leader of
        // bucket 1 negotiated its versions before
        metadataUpdater.setNegotiatedProduceLogVersion((short) 1);
        metadataUpdater.setNegotiatedProduceLogVersion(
                metadataUpdater.leaderFor(DATA1_TABLE_PATH, tb1), (short) 2);
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        long writerId = idempotenceManager.writerId();

        CompletableFuture<Exception> future = new CompletableFuture<>();
        appendToAccumulator(tb2, row(1, "a"), (tb, leo, e) -> future.complete(e));
        sender1.runOnce();
        assertWriterEpochEquals(tb2, (ProduceLogRequest) getRequest(tb2, 0), 0);

        // the leader of bucket 1 has not applied the level yet
        finishIdempotentProduceLogRequest(
                0, tb2, 0, createProduceLogResponse(tb2, Errors.UNSUPPORTED_VERSION));
        sender1.runOnce();
        assertThat(future).isNotDone();

        // the same batch is accepted once the leader applied the level
        assertWriterEpochEquals(tb2, (ProduceLogRequest) getRequest(tb2, 0), 0);
        finishIdempotentProduceLogRequest(0, tb2, 0, createProduceLogResponse(tb2, 0L, 1L));
        sender1.runOnce();
        assertThat(future.get()).isNull();
        assertThat(idempotenceManager.writerId()).isEqualTo(writerId);
        assertThat(idempotenceManager.writerEpoch()).isEqualTo((short) 0);
    }

    private static void assertWriterEpochEquals(
            TableBucket tb, ProduceLogRequest request, int expectedWriterEpoch) {
        LogRecordBatch batch = getProduceLogRecords(request, tb).batchIterator().next();
        assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V3);
        assertThat(batch.writerEpoch()).isEqualTo((short) expectedWriterEpoch);
    }

    private TestingMetadataUpdater initializeMetadataUpdater() {
        Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        tableInfos.put(DATA1_TABLE_PATH, DATA1_TABLE_INFO);
        tableInfos.put(DATA1_TABLE_PATH_PK, DATA1_TABLE_INFO_PK);
        return new TestingMetadataUpdater(tableInfos);
    }

    private void appendToAccumulator(TableBucket tb, GenericRow row, WriteCallback writeCallback)
            throws Exception {
        bucketAssigner.setBucketId(tb.getBucket());
        accumulator.append(
                WriteRecord.forArrowAppend(
                        DATA1_TABLE_INFO, PhysicalTablePath.of(DATA1_TABLE_PATH), row, null),
                writeCallback,
                metadataUpdater.getCluster());
    }

    private void failRequest(TableBucket tb, int index, Throwable throwable) {
        TestTabletServerGateway gateway =
                (TestTabletServerGateway)
                        metadataUpdater.newTabletServerClientForNode(
                                metadataUpdater.leaderFor(DATA1_TABLE_PATH, tb));
        gateway.failRequest(index, throwable);
    }

    private ApiMessage getRequest(TableBucket tb, int index) {
        TestTabletServerGateway gateway =
                (TestTabletServerGateway)
                        metadataUpdater.newTabletServerClientForNode(
                                metadataUpdater.leaderFor(DATA1_TABLE_PATH, tb));
        return gateway.getRequest(index);
    }

    private int pendingRequestSize(TableBucket tb) {
        TestTabletServerGateway gateway =
                (TestTabletServerGateway)
                        metadataUpdater.newTabletServerClientForNode(
                                metadataUpdater.leaderFor(DATA1_TABLE_PATH, tb));
        return gateway.pendingRequestSize();
    }

    private void finishIdempotentProduceLogRequest(
            int batchSequence, TableBucket tb, int index, ProduceLogResponse response) {
        TestTabletServerGateway gateway =
                (TestTabletServerGateway)
                        metadataUpdater.newTabletServerClientForNode(
                                metadataUpdater.leaderFor(DATA1_TABLE_PATH, tb));
        ApiMessage request = getRequest(tb, index);
        assertThat(request).isInstanceOf(ProduceLogRequest.class);
        assertThat(hasIdempotentRecords(tb, (ProduceLogRequest) request)).isTrue();
        assertBatchSequenceEquals(tb, (ProduceLogRequest) request, batchSequence);
        gateway.response(index, response);
    }

    private ProduceLogResponse createProduceLogResponse(
            TableBucket tb, long baseOffset, long endOffset) {
        return makeProduceLogResponse(
                Collections.singletonList(
                        new ProduceLogResultForBucket(tb, baseOffset, endOffset)));
    }

    private ProduceLogResponse createProduceLogResponse(TableBucket tb, Errors error) {
        return makeProduceLogResponse(
                Collections.singletonList(new ProduceLogResultForBucket(tb, error.toApiError())));
    }

    private IdempotenceManager createIdempotenceManager(boolean idempotenceEnabled) {
        return new IdempotenceManager(
                idempotenceEnabled,
                MAX_INFLIGHT_REQUEST_PER_BUCKET,
                metadataUpdater.newRandomTabletServerClient(),
                metadataUpdater);
    }

    private static boolean hasIdempotentRecords(TableBucket tb, ProduceLogRequest request) {
        MemoryLogRecords memoryLogRecords = getProduceLogRecords(request, tb);
        return memoryLogRecords.batchIterator().next().writerId() != NO_WRITER_ID;
    }

    private static void assertBatchSequenceEquals(
            TableBucket tb, ProduceLogRequest request, int expectedBatchSequence) {
        MemoryLogRecords memoryLogRecords = getProduceLogRecords(request, tb);
        assertThat(memoryLogRecords.batchIterator().next().batchSequence())
                .isEqualTo(expectedBatchSequence);
    }

    private static MemoryLogRecords getProduceLogRecords(
            ProduceLogRequest request, TableBucket tableBucket) {
        for (ProduceLogDataForBucket bucketData : toProduceLogDataForBuckets(request)) {
            if (bucketData.tableBucket().equals(tableBucket)) {
                return bucketData.records();
            }
        }
        throw new IllegalArgumentException("No records found for table bucket " + tableBucket);
    }

    private Sender setupWithIdempotenceState(IdempotenceManager idempotenceManager) {
        int reties = Integer.MAX_VALUE;
        int batchTimeoutMs = 0;
        Duration retryBackoff = Duration.ZERO;
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
}
