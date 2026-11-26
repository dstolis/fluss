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

import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the writer epoch of the {@link Sender} for log record batches of magic V3. */
final class SenderWriterEpochTest extends SenderTestBase {
    private final TableBucket tb2 = new TableBucket(DATA1_TABLE_ID, 1);

    /**
     * Batches of magic V3 carry a writer epoch. When the server reports a real gap, for example
     * because it expired the writer state, the writer keeps its id, bumps the epoch and retries the
     * batch from sequence 0, instead of failing the batch and requesting a new writer id.
     */
    @Test
    void testOutOfOrderSequenceBumpsWriterEpochForV3Batches() throws Exception {
        metadataUpdater.setFinalizedFeatureLevel(LOG_RECORD_BATCH_VERSION, 3);
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
        metadataUpdater.setFinalizedFeatureLevel(LOG_RECORD_BATCH_VERSION, 3);
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
     * Two buckets lose their writer state, and the second one reports its gap after the epoch was
     * already bumped for the first one. Its batches are renumbered under the current epoch as well,
     * without another bump, instead of being resent with their old sequence forever.
     */
    @Test
    void testBucketReportingGapAfterEpochBumpIsRenumbered() throws Exception {
        metadataUpdater.setFinalizedFeatureLevel(LOG_RECORD_BATCH_VERSION, 3);
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();

        // one acknowledged batch per bucket, so the next batches have sequence 1
        for (TableBucket bucket : Arrays.asList(tb1, tb2)) {
            appendToAccumulator(bucket, row(1, "a"), (tb, leo, e) -> {});
            sender1.runOnce();
            finishIdempotentProduceLogRequest(
                    0, bucket, 0, createProduceLogResponse(bucket, 0L, 1L));
            sender1.runOnce();
        }

        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(2, "b"), (tb, leo, e) -> future1.complete(e));
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb2, row(2, "b"), (tb, leo, e) -> future2.complete(e));
        sender1.runOnce();

        // bucket 0 reports its gap first, and the epoch is bumped for it
        finishIdempotentProduceLogRequest(
                1, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender1.runOnce();
        assertThat(idempotenceManager.writerEpoch()).isEqualTo((short) 1);
        assertWriterEpochEquals(tb1, (ProduceLogRequest) getRequest(tb1, 0), 1);

        // bucket 1 reports its gap afterwards
        finishIdempotentProduceLogRequest(
                1, tb2, 0, createProduceLogResponse(tb2, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender1.runOnce();
        ProduceLogRequest retried = (ProduceLogRequest) getRequest(tb2, 0);
        assertWriterEpochEquals(tb2, retried, 1);
        assertBatchSequenceEquals(tb2, retried, 0);
        assertThat(idempotenceManager.writerEpoch()).isEqualTo((short) 1);

        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 1L, 2L));
        finishIdempotentProduceLogRequest(0, tb2, 0, createProduceLogResponse(tb2, 1L, 2L));
        sender1.runOnce();
        assertThat(future1.get()).isNull();
        assertThat(future2.get()).isNull();
    }

    /**
     * The epoch bump waits until every batch in flight for the bucket has been answered, since a
     * batch still waiting for its answer may have been written.
     */
    @Test
    void testEpochBumpWaitsForAllResponses() throws Exception {
        metadataUpdater.setFinalizedFeatureLevel(LOG_RECORD_BATCH_VERSION, 3);
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
        metadataUpdater.setFinalizedFeatureLevel(LOG_RECORD_BATCH_VERSION, 3);
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

    /**
     * A gap reported for a batch that was not written asks to renumber its bucket, but a later
     * batch of the bucket has an unknown outcome, so the bucket can't be renumbered. The writer id
     * is reset and the batches fail, instead of the first batch being resent with the same sequence
     * forever.
     */
    @Test
    void testRestartBlockedByUnknownOutcomeResetsWriterId() throws Exception {
        metadataUpdater.setFinalizedFeatureLevel(LOG_RECORD_BATCH_VERSION, 3);
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        long writerId = idempotenceManager.writerId();

        CompletableFuture<Exception> future0 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future0.complete(e));
        sender1.runOnce();
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(2, "b"), (tb, leo, e) -> future1.complete(e));
        sender1.runOnce();
        assertThat(pendingRequestSize(tb1)).isEqualTo(2);

        // the request of batch 1 times out, then batch 0 is rejected for a gap
        failRequest(tb1, 1, new TimeoutException("request timed out"));
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender1.runOnce();
        assertThat(idempotenceManager.writerId()).isNotEqualTo(writerId);

        // the server keeps reporting the gap for whatever is resent under the old writer id
        for (int i = 0; i < 10 && !(future0.isDone() && future1.isDone()); i++) {
            sender1.runOnce();
            while (pendingRequestSize(tb1) > 0) {
                leaderGateway(tb1)
                        .response(
                                0,
                                createProduceLogResponse(
                                        tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
            }
        }
        assertThat(future0).isDone();
        assertThat(future1).isDone();
        assertThat(future0.get()).isNotNull();
        assertThat(future1.get()).isNotNull();
    }

    /** Without V3, batches carry no epoch, so a real gap still fails the batch and resets. */
    @Test
    void testOutOfOrderSequenceResetsWriterWithoutV3() throws Exception {
        metadataUpdater.setFinalizedFeatureLevel(LOG_RECORD_BATCH_VERSION, 2);
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
     * A batch that was created before the writer switched to V3 carries no epoch. Once the cluster
     * allows V3, a real gap for it is recovered with a new writer id, under which the batch is sent
     * again from sequence 0.
     */
    @Test
    void testGapForBatchWithoutEpochRenewsWriterIdOnceV3IsAllowed() throws Exception {
        metadataUpdater.setFinalizedFeatureLevel(LOG_RECORD_BATCH_VERSION, 2);
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        long writerId = idempotenceManager.writerId();

        // the batch is created in an older format, then the cluster allows V3
        CompletableFuture<Exception> future = new CompletableFuture<>();
        appendToAccumulator(tb1, row(1, "a"), (tb, leo, e) -> future.complete(e));
        metadataUpdater.setFinalizedFeatureLevel(LOG_RECORD_BATCH_VERSION, 3);
        sender1.runOnce();
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        assertThat(future).isNotDone();

        sender1.runOnce();
        long newWriterId = idempotenceManager.writerId();
        assertThat(newWriterId).isNotEqualTo(writerId);
        LogRecordBatch retried =
                getProduceLogRecords((ProduceLogRequest) getRequest(tb1, 0), tb1)
                        .batchIterator()
                        .next();
        assertThat(retried.magic()).isLessThan(LOG_MAGIC_VALUE_V3);
        assertThat(retried.writerId()).isEqualTo(newWriterId);
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce();
        assertThat(future.get()).isNull();
    }

    /**
     * A writer writes V3 once one tablet server reports that the cluster allows it, also to the
     * leaders it connected to before. A leader that has not applied the level yet rejects the batch
     * before writing it, so the batch is resent as it is until the leader accepts it.
     */
    @Test
    void testV3BatchRejectedBeforeLeaderAppliedTheLevelIsRetried() throws Exception {
        // one tablet server reported the level, the leader of bucket 1 has not applied it yet
        metadataUpdater.setFinalizedFeatureLevel(LOG_RECORD_BATCH_VERSION, 3);
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
}
