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
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.entity.PutKvDataForBucket;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makePutKvResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toPutKvDataForBuckets;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests how the {@link Sender} restarts the sequence of a bucket whose batches carry no writer
 * epoch, such as the KV batches of a primary key table: it requests a new writer id and renumbers
 * the batches the server rejected under it.
 */
final class SenderWriterIdRenewalTest {
    private static final int TOTAL_MEMORY_SIZE = 1024 * 1024;
    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final int BATCH_SIZE = 16 * 1024;
    private static final int PAGE_SIZE = 256;
    private static final int REQUEST_TIMEOUT = 5000;
    private static final short ACKS_ALL = -1;
    private static final int MAX_INFLIGHT_REQUEST_PER_BUCKET = 5;

    private final TableBucket tb1 = new TableBucket(DATA1_TABLE_ID_PK, 0);
    private final TableBucket tb2 = new TableBucket(DATA1_TABLE_ID_PK, 1);

    private TestingMetadataUpdater metadataUpdater;
    private IdempotenceManager idempotenceManager;
    private RecordAccumulator accumulator;
    private TestingBucketAssigner bucketAssigner;
    private Sender sender;

    @BeforeEach
    void setup() throws Exception {
        Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        tableInfos.put(DATA1_TABLE_PATH, DATA1_TABLE_INFO);
        tableInfos.put(DATA1_TABLE_PATH_PK, DATA1_TABLE_INFO_PK);
        metadataUpdater = new TestingMetadataUpdater(tableInfos);
        // the cluster allows V3 batches, from when on writers recover from a lost writer state
        metadataUpdater.setFinalizedFeatureLevel(LOG_RECORD_BATCH_VERSION, 3);
        idempotenceManager =
                new IdempotenceManager(
                        true,
                        MAX_INFLIGHT_REQUEST_PER_BUCKET,
                        metadataUpdater.newRandomTabletServerClient(),
                        metadataUpdater);
        sender = createSender();
        // requests the first writer id
        sender.runOnce();
    }

    /**
     * The server lost the writer state of the bucket, for example because it expired, and reports a
     * gap for the next batch. The batch was not written, so instead of failing it the writer
     * requests a new writer id and sends it again from sequence 0.
     */
    @Test
    void testGapRenewsWriterIdForKvBatch() throws Exception {
        long writerId = idempotenceManager.writerId();

        CompletableFuture<Exception> future0 = appendKv(tb1, 1);
        sender.runOnce();
        assertWriterState(tb1, 0, writerId, 0);
        respond(tb1, 0, written(tb1, 1L));
        sender.runOnce();
        assertThat(future0.get()).isNull();

        CompletableFuture<Exception> future1 = appendKv(tb1, 2);
        sender.runOnce();
        assertWriterState(tb1, 0, writerId, 1);
        respond(tb1, 0, rejected(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        assertThat(future1).isNotDone();

        // the next round requests a new writer id and resends the batch from sequence 0
        sender.runOnce();
        long newWriterId = idempotenceManager.writerId();
        assertThat(newWriterId).isNotEqualTo(writerId);
        assertWriterState(tb1, 0, newWriterId, 0);
        respond(tb1, 0, written(tb1, 2L));
        sender.runOnce();
        assertThat(future1.get()).isNull();

        // later batches continue under the new writer id
        CompletableFuture<Exception> future2 = appendKv(tb1, 3);
        sender.runOnce();
        assertWriterState(tb1, 0, newWriterId, 1);
        respond(tb1, 0, written(tb1, 3L));
        sender.runOnce();
        assertThat(future2.get()).isNull();
        assertThat(idempotenceManager.writerId()).isEqualTo(newWriterId);
    }

    /** Before the cluster allows V3 batches, a gap fails the batch and resets the writer id. */
    @Test
    void testGapFailsKvBatchBeforeV3IsAllowed() throws Exception {
        metadataUpdater.setFinalizedFeatureLevel(LOG_RECORD_BATCH_VERSION, 2);
        CompletableFuture<Exception> future0 = appendKv(tb1, 1);
        sender.runOnce();
        respond(tb1, 0, written(tb1, 1L));
        sender.runOnce();
        assertThat(future0.get()).isNull();

        CompletableFuture<Exception> future1 = appendKv(tb1, 2);
        sender.runOnce();
        respond(tb1, 0, rejected(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        assertThat(future1.get()).isInstanceOf(OutOfOrderSequenceException.class);
        assertThat(idempotenceManager.isWriterIdValid()).isFalse();
    }

    /**
     * The leader answers that it timed out waiting for its followers, after it appended the batch.
     * The batch may be stored, so a gap reported for its retry does not renumber it: the batch
     * fails and the writer id is reset.
     */
    @Test
    void testTimeoutAnsweredByLeaderLeavesOutcomeUnknown() throws Exception {
        CompletableFuture<Exception> future0 = appendKv(tb1, 1);
        sender.runOnce();
        respond(tb1, 0, written(tb1, 1L));
        sender.runOnce();
        assertThat(future0.get()).isNull();

        long writerId = idempotenceManager.writerId();
        CompletableFuture<Exception> future1 = appendKv(tb1, 2);
        sender.runOnce();
        respond(tb1, 0, rejected(tb1, Errors.REQUEST_TIME_OUT));
        sender.runOnce();
        assertWriterState(tb1, 0, writerId, 1);

        respond(tb1, 0, rejected(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        assertThat(future1.get()).isInstanceOf(OutOfOrderSequenceException.class);
        assertThat(idempotenceManager.isWriterIdValid()).isFalse();
    }

    /** The batches still in flight for the bucket are renumbered and resent in order. */
    @Test
    void testRenewalRenumbersInFlightBatchesInOrder() throws Exception {
        long writerId = idempotenceManager.writerId();
        CompletableFuture<Exception> future0 = appendKv(tb1, 1);
        sender.runOnce();
        CompletableFuture<Exception> future1 = appendKv(tb1, 2);
        sender.runOnce();
        assertThat(pendingRequests(tb1)).isEqualTo(2);

        // batch 0 hits a gap, batch 1 fails because batch 0 is missing
        respond(tb1, 0, rejected(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        respond(tb1, 0, rejected(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        assertThat(future0).isNotDone();
        assertThat(future1).isNotDone();

        sender.runOnce();
        long newWriterId = idempotenceManager.writerId();
        assertThat(newWriterId).isNotEqualTo(writerId);
        assertThat(pendingRequests(tb1)).isEqualTo(1);
        assertWriterState(tb1, 0, newWriterId, 0);
        respond(tb1, 0, written(tb1, 1L));
        sender.runOnce();
        assertThat(future0.get()).isNull();
        assertWriterState(tb1, 0, newWriterId, 1);
        respond(tb1, 0, written(tb1, 2L));
        sender.runOnce();
        assertThat(future1.get()).isNull();
        // a single new writer id, although both batches were rejected
        assertThat(idempotenceManager.writerId()).isEqualTo(newWriterId);
    }

    /**
     * The renewal waits until every batch in flight for the bucket has been answered, since a batch
     * still waiting for its answer may have been written.
     */
    @Test
    void testRenewalWaitsForAllResponses() throws Exception {
        long writerId = idempotenceManager.writerId();
        CompletableFuture<Exception> future0 = appendKv(tb1, 1);
        sender.runOnce();
        CompletableFuture<Exception> future1 = appendKv(tb1, 2);
        sender.runOnce();

        respond(tb1, 0, rejected(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender.runOnce();
        // batch 1 is still waiting for its answer, so nothing is renumbered or resent yet
        assertThat(idempotenceManager.writerId()).isEqualTo(writerId);
        assertThat(pendingRequests(tb1)).isEqualTo(1);
        assertWriterState(tb1, 0, writerId, 1);

        respond(tb1, 0, rejected(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender.runOnce();
        long newWriterId = idempotenceManager.writerId();
        assertThat(newWriterId).isNotEqualTo(writerId);
        assertWriterState(tb1, 0, newWriterId, 0);
        respond(tb1, 0, written(tb1, 1L));
        sender.runOnce();
        respond(tb1, 0, written(tb1, 2L));
        sender.runOnce();
        assertThat(future0.get()).isNull();
        assertThat(future1.get()).isNull();
    }

    /**
     * A batch whose request got no answer may have been written. If the server then reports a gap
     * for it, it is not renumbered under a new writer id, which could write it twice: the batch
     * fails and the writer id is reset, as before.
     */
    @Test
    void testUnknownOutcomeFailsBatchInsteadOfRenewing() throws Exception {
        long writerId = idempotenceManager.writerId();
        CompletableFuture<Exception> future0 = appendKv(tb1, 1);
        sender.runOnce();
        respond(tb1, 0, written(tb1, 1L));
        sender.runOnce();
        assertThat(future0.get()).isNull();

        // the request of batch 1 times out, so it may or may not have been written
        CompletableFuture<Exception> future1 = appendKv(tb1, 2);
        sender.runOnce();
        gateway(tb1).failRequest(0, new TimeoutException("request timed out"));
        sender.runOnce();

        // the retry is sent as it was, and finds the writer state expired on the server
        assertWriterState(tb1, 0, writerId, 1);
        respond(tb1, 0, rejected(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        assertThat(future1.get()).isInstanceOf(OutOfOrderSequenceException.class);
        assertThat(idempotenceManager.isWriterIdValid()).isFalse();
    }

    /**
     * A new writer id only restarts the bucket that reported the gap. The batches of another bucket
     * that are in flight under the previous writer id complete or are retried under it, and the
     * bucket moves to the new writer id with its next batch.
     */
    @Test
    void testOtherBucketFinishesItsBatchesUnderPreviousWriterId() throws Exception {
        long writerId = idempotenceManager.writerId();
        CompletableFuture<Exception> future1 = appendKv(tb1, 1);
        CompletableFuture<Exception> future2 = appendKv(tb2, 2);
        sender.runOnce();
        assertWriterState(tb1, 0, writerId, 0);
        assertWriterState(tb2, 0, writerId, 0);

        // only bucket 0 lost its writer state
        respond(tb1, 0, rejected(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender.runOnce();
        long newWriterId = idempotenceManager.writerId();
        assertThat(newWriterId).isNotEqualTo(writerId);
        assertWriterState(tb1, 0, newWriterId, 0);

        // the batch of bucket 1 is retried as it was after a retriable error
        respond(tb2, 0, rejected(tb2, Errors.NOT_ENOUGH_REPLICAS_EXCEPTION));
        sender.runOnce();
        assertThat(future2).isNotDone();
        assertWriterState(tb2, 0, writerId, 0);
        respond(tb2, 0, written(tb2, 1L));
        respond(tb1, 0, written(tb1, 1L));
        sender.runOnce();
        assertThat(future1.get()).isNull();
        assertThat(future2.get()).isNull();

        // the next batch of bucket 1 starts a sequence under the new writer id
        CompletableFuture<Exception> future3 = appendKv(tb2, 3);
        sender.runOnce();
        assertWriterState(tb2, 0, newWriterId, 0);
        respond(tb2, 0, written(tb2, 2L));
        sender.runOnce();
        assertThat(future3.get()).isNull();
    }

    /**
     * Two buckets lose their writer state, and the second one reports its gap after the writer id
     * was already renewed for the first one. Its batch restarts under that writer id, without
     * requesting another one.
     */
    @Test
    void testBucketReportingGapAfterRenewalUsesCurrentWriterId() throws Exception {
        long writerId = idempotenceManager.writerId();
        CompletableFuture<Exception> future1 = appendKv(tb1, 1);
        CompletableFuture<Exception> future2 = appendKv(tb2, 2);
        sender.runOnce();

        respond(tb1, 0, rejected(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender.runOnce();
        long newWriterId = idempotenceManager.writerId();
        assertThat(newWriterId).isNotEqualTo(writerId);
        assertWriterState(tb1, 0, newWriterId, 0);

        respond(tb2, 0, rejected(tb2, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender.runOnce();
        assertWriterState(tb2, 0, newWriterId, 0);
        assertThat(idempotenceManager.writerId()).isEqualTo(newWriterId);

        respond(tb1, 0, written(tb1, 1L));
        respond(tb2, 0, written(tb2, 1L));
        sender.runOnce();
        assertThat(future1.get()).isNull();
        assertThat(future2.get()).isNull();
    }

    private CompletableFuture<Exception> appendKv(TableBucket tb, int id) throws Exception {
        BinaryRow row = compactedRow(DATA1_TABLE_INFO_PK.getRowType(), new Object[] {id, "v"});
        byte[] key =
                new CompactedKeyEncoder(
                                DATA1_TABLE_INFO_PK.getRowType(),
                                DATA1_TABLE_INFO_PK.getSchema().getPrimaryKeyIndexes())
                        .encodeKey(row);
        CompletableFuture<Exception> future = new CompletableFuture<>();
        bucketAssigner.setBucketId(tb.getBucket());
        accumulator.append(
                WriteRecord.forUpsert(
                        DATA1_TABLE_INFO_PK,
                        PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                        row,
                        key,
                        key,
                        WriteFormat.COMPACTED_KV,
                        null),
                (tableBucket, logEndOffset, error) -> future.complete(error),
                metadataUpdater.getCluster());
        return future;
    }

    /** Asserts the writer id and sequence of the batch in the given pending request. */
    private void assertWriterState(TableBucket tb, int index, long writerId, int batchSequence) {
        PutKvRequest request = (PutKvRequest) gateway(tb).getRequest(index);
        KvRecordBatch batch = null;
        for (PutKvDataForBucket bucketData : toPutKvDataForBuckets(request)) {
            if (bucketData.tableBucket().equals(tb)) {
                batch = bucketData.records();
            }
        }
        assertThat(batch).isNotNull();
        assertThat(batch.writerId()).isEqualTo(writerId);
        assertThat(batch.batchSequence()).isEqualTo(batchSequence);
    }

    private void respond(TableBucket tb, int index, PutKvResponse response) {
        gateway(tb).response(index, response);
    }

    private int pendingRequests(TableBucket tb) {
        return gateway(tb).pendingRequestSize();
    }

    private TestTabletServerGateway gateway(TableBucket tb) {
        return (TestTabletServerGateway)
                metadataUpdater.newTabletServerClientForNode(
                        metadataUpdater.leaderFor(DATA1_TABLE_PATH_PK, tb));
    }

    private static PutKvResponse written(TableBucket tb, long logEndOffset) {
        return makePutKvResponse(
                Collections.singletonList(new PutKvResultForBucket(tb, logEndOffset)));
    }

    private static PutKvResponse rejected(TableBucket tb, Errors error) {
        return makePutKvResponse(
                Collections.singletonList(new PutKvResultForBucket(tb, error.toApiError())));
    }

    private Sender createSender() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, new MemorySize(TOTAL_MEMORY_SIZE));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, new MemorySize(BATCH_SIZE));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, new MemorySize(PAGE_SIZE));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT, Duration.ZERO);
        conf.set(ConfigOptions.CLIENT_WRITER_RETRY_BACKOFF, Duration.ZERO);
        conf.set(ConfigOptions.CLIENT_WRITER_RETRY_BACKOFF_MAX, Duration.ZERO);
        TestingWriterMetricGroup writerMetricGroup = TestingWriterMetricGroup.newInstance();
        bucketAssigner = new TestingBucketAssigner();
        accumulator =
                new RecordAccumulator(
                        conf,
                        idempotenceManager,
                        writerMetricGroup,
                        SystemClock.getInstance(),
                        new ExponentialBackoff(1000, 2, 10000, 0),
                        (tableInfo, path) -> bucketAssigner);
        return new Sender(
                accumulator,
                REQUEST_TIMEOUT,
                MAX_REQUEST_SIZE,
                ACKS_ALL,
                Integer.MAX_VALUE,
                metadataUpdater,
                idempotenceManager,
                writerMetricGroup);
    }
}
