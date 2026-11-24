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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.InvalidMetadataException;
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.exception.UnknownWriterIdException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.InitWriterRequest;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_EPOCH;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A class which manages the idempotence in writer keeps the state necessary to ensure idempotent of
 * writer.
 */
@Internal
@ThreadSafe
public class IdempotenceManager {
    private static final Logger LOG = LoggerFactory.getLogger(IdempotenceManager.class);

    // retry config for requesting writerId, maybe make it as configurable
    private static final int RETRY_TIMES = 3;
    private static final int RETRY_INTERVAL_MS = 100;

    private final boolean idempotenceEnabled;
    private final IdempotenceBucketMap idempotenceBucketMap;
    private final int maxInflightRequestsPerBucket;
    private final TabletServerGateway tabletServerGateway;
    private final MetadataUpdater metadataUpdater;

    private volatile long writerId;

    /**
     * The current epoch of the writer. It is bumped locally, without asking the server, to restart
     * the sequence of a bucket whose batches all carry a writer epoch.
     */
    private volatile short writerEpoch;

    /**
     * The buckets whose sequence is restarted at 0 before the next batches are drained, mapped to
     * whether every batch that requested it carries a writer epoch.
     */
    @GuardedBy("this")
    private final Map<TableBucket, Boolean> bucketsToRestart = new HashMap<>();

    /**
     * Whether new log record batches are written in magic V3. It turns true once a tablet server
     * reported that the cluster finalized the level that allows V3, either in the finalized
     * features of its API versions or by advertising the {@code PRODUCE_LOG} version that carries
     * V3 batches. It then stays true: the level can't be lowered.
     */
    private volatile boolean recordBatchV3Enabled;

    public IdempotenceManager(
            boolean idempotenceEnabled,
            int maxInflightRequestsPerBucket,
            TabletServerGateway tabletServerGateway,
            MetadataUpdater metadataUpdater) {
        this.idempotenceEnabled = idempotenceEnabled;
        this.maxInflightRequestsPerBucket = maxInflightRequestsPerBucket;
        this.idempotenceBucketMap = new IdempotenceBucketMap();
        this.tabletServerGateway = tabletServerGateway;
        this.writerId = NO_WRITER_ID;
        this.writerEpoch = NO_WRITER_EPOCH;
        this.metadataUpdater = metadataUpdater;
    }

    boolean idempotenceEnabled() {
        return idempotenceEnabled;
    }

    boolean hasWriterId(long writerId) {
        return this.writerId == writerId;
    }

    long writerId() {
        return writerId;
    }

    void setWriterId(long writerId) {
        LOG.info("WriterId set to {}", writerId);
        this.writerId = writerId;
        this.writerEpoch = NO_WRITER_EPOCH;
    }

    short writerEpoch() {
        return writerEpoch;
    }

    /** The writer epoch to set on the next batch of the given bucket. */
    synchronized short writerEpoch(TableBucket tableBucket) {
        return idempotenceBucketMap.getOrCreate(tableBucket).writerEpoch();
    }

    /**
     * Whether new log record batches should be written in magic V3, so they carry the writer epoch.
     * Only idempotent writers use V3, and only once a tablet server has reported that the cluster
     * finalized the level that allows it. Levels are never lowered, so the answer stays true.
     */
    boolean writeRecordBatchV3() {
        if (!idempotenceEnabled) {
            return false;
        }
        if (recordBatchV3Enabled) {
            return true;
        }
        if (metadataUpdater == null) {
            return false;
        }
        if (metadataUpdater.finalizedFeatureLevel(LOG_RECORD_BATCH_VERSION) >= LOG_MAGIC_VALUE_V3) {
            LOG.info(
                    "The cluster allows log record batches of magic V3, writing them from now on.");
            recordBatchV3Enabled = true;
            return true;
        }
        return false;
    }

    boolean isWriterIdValid() {
        return writerId > NO_WRITER_ID;
    }

    synchronized void maybeUpdateWriterState(TableBucket tableBucket, WriteBatch nextBatch) {
        if (hasStaleWriterState(tableBucket, nextBatch) && !hasInflightBatches(tableBucket)) {
            // If the bucket was on a different writer id or epoch and all its in-flight batches
            // have completed, reset the bucket batch sequence so that the next batch starts from 0.
            short epoch = nextBatch.hasWriterEpoch() ? writerEpoch : NO_WRITER_EPOCH;
            idempotenceBucketMap.startBatchSequencesAtBeginning(tableBucket, writerId, epoch);
            LOG.debug(
                    "Writer id of bucket {} set to {} with epoch {}. Reinitialize batch sequence at beginning.",
                    tableBucket,
                    writerId,
                    epoch);
        }
    }

    /**
     * Whether the bucket's sequences belong to an older writer id, or to an older epoch while the
     * next batch can carry the current one. Batches without a writer epoch keep counting under the
     * bucket's epoch, which the server already knows.
     */
    synchronized boolean hasStaleWriterState(TableBucket tableBucket, WriteBatch nextBatch) {
        IdempotenceBucketEntry entry = idempotenceBucketMap.getOrCreate(tableBucket);
        return writerId != entry.writerId()
                || (nextBatch.hasWriterEpoch() && writerEpoch != entry.writerEpoch());
    }

    /** Whether the in-flight batches of the bucket are about to be renumbered. */
    synchronized boolean isWaitingForSequenceRestart(TableBucket tableBucket) {
        return bucketsToRestart.containsKey(tableBucket);
    }

    /**
     * Requests to restart the sequence of the bucket at 0, because the server reported a gap in the
     * sequence or an unknown writer for one of its batches, for example after it expired the writer
     * state. The restart happens before the next drain, see {@link #maybeRestartSequences}.
     *
     * @param batchCarriesEpoch whether the rejected batch carries a writer epoch
     */
    synchronized void requestSequenceRestart(TableBucket tableBucket, boolean batchCarriesEpoch) {
        LOG.info(
                "Requesting to restart the sequence of table-bucket {} of writer {} with epoch {}.",
                tableBucket,
                writerId,
                writerEpoch);
        bucketsToRestart.merge(tableBucket, batchCarriesEpoch, Boolean::logicalAnd);
    }

    /**
     * Restarts the sequence of the buckets that requested it. Their in-flight batches are
     * renumbered from sequence 0 under an identity the server has not seen for the bucket, so it
     * accepts them as a new sequence:
     *
     * <ul>
     *   <li>A bumped epoch, if every batch of the bucket carries a writer epoch, which only log
     *       record batches of magic V3 do. This does not ask the server.
     *   <li>Otherwise a new writer id, for example for KV batches, which is requested from the
     *       server.
     * </ul>
     *
     * <p>Other buckets keep their sequence, and move to the new epoch or writer id once their
     * in-flight batches have completed, see {@link #maybeUpdateWriterState}. A bucket that requests
     * a restart after the epoch or the writer id has already moved on restarts under the current
     * one.
     *
     * <p>A bucket is only restarted once every request carrying one of its batches has been
     * answered. Batches the server rejected were not written and are resent. A batch whose request
     * got no answer, for example on a timeout, may have been written, so it is never renumbered:
     * the restart of its bucket is cancelled, the batch is retried as it is, and if the server
     * reports a gap for it, the batch fails and the writer id is reset, as for other unknown
     * outcomes.
     */
    void maybeRestartSequences(Set<PhysicalTablePath> tablePaths) throws Throwable {
        List<TableBucket> ready;
        synchronized (this) {
            ready = bucketsReadyToRestart();
            if (ready.isEmpty()) {
                return;
            }
            if (!needsNewWriterId(ready)) {
                restartSequences(ready);
                return;
            }
        }
        // Requested outside the lock, so the responses of other buckets are handled meanwhile. The
        // ready buckets have no request in flight, and the sender, which runs this method, drains
        // nothing before it returns.
        long newWriterId = requestWriterId(tablePaths);
        synchronized (this) {
            setWriterId(newWriterId);
            restartSequences(ready);
        }
    }

    /** The buckets to restart whose batches have all been answered by the server. */
    @GuardedBy("this")
    private List<TableBucket> bucketsReadyToRestart() {
        List<TableBucket> ready = new ArrayList<>();
        Iterator<TableBucket> buckets = bucketsToRestart.keySet().iterator();
        while (buckets.hasNext()) {
            TableBucket tableBucket = buckets.next();
            if (!idempotenceBucketMap.contains(tableBucket)) {
                buckets.remove();
                continue;
            }
            IdempotenceBucketEntry entry = idempotenceBucketMap.get(tableBucket);
            if (entry.hasBatchAwaitingResponse()) {
                // wait for every answer, a batch still in flight may have been written
                continue;
            }
            if (entry.hasBatchWithUnknownOutcome()) {
                // A batch of the bucket may have been written, so it must not be renumbered.
                // Cancel the restart: the batch is retried as it is, and if the server reports a
                // gap for it, it fails and the writer id is reset.
                LOG.info(
                        "Not restarting the sequence of table-bucket {} of writer {}, one of its "
                                + "batches may have been written.",
                        tableBucket,
                        writerId);
                buckets.remove();
                continue;
            }
            ready.add(tableBucket);
        }
        return ready;
    }

    /**
     * Whether one of the buckets can only restart under a new writer id: it is on the current
     * writer id, and an epoch can't tell its restart apart from its old sequence, because not all
     * of its batches carry one or the epoch is exhausted.
     */
    @GuardedBy("this")
    private boolean needsNewWriterId(List<TableBucket> buckets) {
        for (TableBucket tableBucket : buckets) {
            IdempotenceBucketEntry entry = idempotenceBucketMap.get(tableBucket);
            if (entry.writerId() != writerId) {
                // the writer id was renewed since, the bucket restarts under it
                continue;
            }
            if (!carriesWriterEpoch(tableBucket, entry)
                    || (entry.writerEpoch() == writerEpoch && writerEpoch == Short.MAX_VALUE)) {
                return true;
            }
        }
        return false;
    }

    /** Renumbers the in-flight batches of the buckets from sequence 0. */
    @GuardedBy("this")
    private void restartSequences(List<TableBucket> buckets) {
        boolean bumpEpoch = false;
        for (TableBucket tableBucket : buckets) {
            if (idempotenceBucketMap.contains(tableBucket)) {
                IdempotenceBucketEntry entry = idempotenceBucketMap.get(tableBucket);
                // a bucket behind the current writer id or epoch restarts under the current one
                bumpEpoch |=
                        entry.writerId() == writerId
                                && entry.writerEpoch() == writerEpoch
                                && carriesWriterEpoch(tableBucket, entry);
            }
        }
        if (bumpEpoch) {
            writerEpoch = (short) (writerEpoch + 1);
            LOG.info("Bumped the epoch of writer {} to {}.", writerId, writerEpoch);
        }
        for (TableBucket tableBucket : buckets) {
            // the sequences of all buckets are discarded if the writer id was reset meanwhile
            if (idempotenceBucketMap.contains(tableBucket)) {
                IdempotenceBucketEntry entry = idempotenceBucketMap.get(tableBucket);
                short epoch =
                        carriesWriterEpoch(tableBucket, entry) ? writerEpoch : NO_WRITER_EPOCH;
                idempotenceBucketMap.startBatchSequencesAtBeginning(tableBucket, writerId, epoch);
                LOG.info(
                        "Restarted the sequence of table-bucket {} under writer id {} and epoch {}.",
                        tableBucket,
                        writerId,
                        epoch);
            }
            bucketsToRestart.remove(tableBucket);
        }
    }

    /** Whether every batch of the bucket that is renumbered carries a writer epoch. */
    @GuardedBy("this")
    private boolean carriesWriterEpoch(TableBucket tableBucket, IdempotenceBucketEntry entry) {
        return bucketsToRestart.getOrDefault(tableBucket, true)
                && entry.inflightBatchesHaveWriterEpoch();
    }

    /**
     * This method is used when writer needs to reset its internal state because of an irrecoverable
     * exception from the tablet server.
     *
     * <p>We need to reset writer id and associated state when we have sent a batch to the tablet
     * server, but we either get a non-retriable exception or we run out of retries, or the batch
     * expired in writer queue after it was already sent to the tablet server.
     *
     * <p>In all of these cases, we don't know whether batch was actually committed on the tablet
     * server, and hence whether the batch sequence was actually updated. If we don't reset the
     * writer state, we risk the chance that all future messages will return an {@link
     * OutOfOrderSequenceException}.
     */
    synchronized void resetWriterId() {
        setWriterId(NO_WRITER_ID);
        this.bucketsToRestart.clear();
        this.idempotenceBucketMap.reset();
    }

    synchronized int nextSequence(TableBucket tableBucket) {
        return idempotenceBucketMap.getOrCreate(tableBucket).nextSequence();
    }

    synchronized void incrementBatchSequence(TableBucket tableBucket) {
        idempotenceBucketMap.get(tableBucket).incrementSequence();
    }

    synchronized WriteBatch nextBatchBySequence(TableBucket tableBucket) {
        return idempotenceBucketMap.nextBatchBySequence(tableBucket);
    }

    synchronized boolean isNextSequence(TableBucket tableBucket, int sequence) {
        return sequence
                        - lastAckedBatchSequence(tableBucket)
                                .orElse(IdempotenceBucketEntry.NO_LAST_ACKED_BATCH_SEQUENCE)
                == 1;
    }

    synchronized Optional<Integer> lastAckedBatchSequence(TableBucket tableBucket) {
        return idempotenceBucketMap.lastAckedBatchSequence(tableBucket);
    }

    synchronized void addInFlightBatch(WriteBatch batch, TableBucket tableBucket) {
        if (!batch.hasBatchSequence()) {
            throw new IllegalStateException(
                    "Can't track batch for bucket "
                            + tableBucket
                            + " when batch sequence is not set.");
        }
        idempotenceBucketMap.get(tableBucket).addInflightBatch(batch);
    }

    synchronized void removeInFlightBatch(ReadyWriteBatch batch) {
        if (hasInflightBatches(batch.tableBucket())) {
            idempotenceBucketMap.removeInFlightBatch(batch);
        }
    }

    /**
     * Returns the first inflight batch sequence for a given bucket. This is the base sequence of an
     * inflight batch with the lowest batch sequence.
     *
     * @return the lowest inflight batch sequence if the idempotence manager is tracking inflight
     *     requests for this bucket. If there are no inflight requests being tracked for this
     *     bucket, this method will return LogRecordBatch.NO_BATCH_SEQUENCE.
     */
    synchronized int firstInFlightBatchSequence(TableBucket tableBucket) {
        if (!hasInflightBatches(tableBucket)) {
            return NO_BATCH_SEQUENCE;
        }
        WriteBatch batch = nextBatchBySequence(tableBucket);
        return batch == null ? NO_BATCH_SEQUENCE : batch.batchSequence();
    }

    synchronized void handleCompletedBatch(ReadyWriteBatch readyWriteBatch) {
        TableBucket tableBucket = readyWriteBatch.tableBucket();
        WriteBatch batch = readyWriteBatch.writeBatch();
        if (!isOnWriterIdOfBucket(tableBucket, batch)) {
            LOG.debug(
                    "Ignoring completed batch {} with writer id {}, and batch sequence {} "
                            + "since the writer id has been reset internally",
                    batch,
                    batch.writerId(),
                    batch.batchSequence());
            return;
        }

        int lastAckedSequence = maybeUpdateLastAckedSequence(tableBucket, batch.batchSequence());
        LOG.debug(
                "Writer id: {}; Set last ack'd batch sequence for table-bucket {} to {}",
                batch.writerId(),
                tableBucket,
                lastAckedSequence);
        removeInFlightBatch(readyWriteBatch);
    }

    synchronized void handleFailedBatch(
            ReadyWriteBatch readyWriteBatch, Exception exception, boolean adjustSequenceNumbers) {
        TableBucket tableBucket = readyWriteBatch.tableBucket();
        WriteBatch batch = readyWriteBatch.writeBatch();
        if (!isOnWriterIdOfBucket(tableBucket, batch)) {
            LOG.debug(
                    "Ignoring failed batch {} with writer id {}, and batch sequence {} "
                            + "since the writer id has been reset internally",
                    batch,
                    batch.writerId(),
                    batch.batchSequence(),
                    exception);
            return;
        }

        if (exception instanceof OutOfOrderSequenceException
                || exception instanceof UnknownWriterIdException) {
            LOG.warn(
                    "The server returned {} for table-bucket {} with writer id {}, epoch {} and batch sequence {}.",
                    exception,
                    tableBucket,
                    batch.writerId(),
                    batch.writerEpoch(),
                    batch.batchSequence());
            if (batch.isOutcomeUnknown() || !restartsSequenceOnGap(batch)) {
                // An earlier attempt of this batch may have been written, or the sequence of the
                // bucket can't be restarted, so no guarantee can be made about the batches behind
                // it. Discard the writer id and the sequences of all buckets.
                resetWriterId();
            } else {
                // The batch was not written. Restart the sequence of the bucket, so the batches
                // behind it are not rejected for the gap it leaves.
                removeInFlightBatch(readyWriteBatch);
                requestSequenceRestart(tableBucket, batch.hasWriterEpoch());
            }
        } else {
            removeInFlightBatch(readyWriteBatch);
            if (adjustSequenceNumbers) {
                idempotenceBucketMap.adjustSequencesDueToFailedBatch(readyWriteBatch);
            }
        }
    }

    /**
     * Whether a gap reported for the batch restarts the sequence of its bucket instead of failing
     * the batch. A batch with a writer epoch restarts under a bumped epoch. A batch without one,
     * such as a KV batch, restarts under a new writer id, but only once the cluster allows V3
     * batches: before that, a gap fails the batch and resets the writer id, as it always did.
     */
    private boolean restartsSequenceOnGap(WriteBatch batch) {
        return batch.hasWriterEpoch() || writeRecordBatchV3();
    }

    /**
     * Whether the batch was numbered under the writer id the bucket is on. It is not once the
     * writer id was reset, which discards the sequences of all buckets. Buckets can be on different
     * writer ids for a while: after a new writer id was requested to restart one bucket, the others
     * move to it once their in-flight batches have completed.
     */
    synchronized boolean isOnWriterIdOfBucket(TableBucket tableBucket, WriteBatch batch) {
        return idempotenceBucketMap.getOrCreate(tableBucket).writerId() == batch.writerId();
    }

    synchronized boolean hasInflightBatches(TableBucket tableBucket) {
        return idempotenceBucketMap.getOrCreate(tableBucket).hasInflightBatches();
    }

    synchronized boolean canSendMoreRequests(TableBucket tableBucket) {
        return inflightBatchSize(tableBucket) < maxInflightRequestsPerBucket;
    }

    @VisibleForTesting
    synchronized int inflightBatchSize(TableBucket tableBucket) {
        return idempotenceBucketMap.getOrCreate(tableBucket).inflightBatchSize();
    }

    synchronized boolean canRetry(WriteBatch batch, TableBucket tableBucket, Errors error) {
        if (!isWriterIdValid()) {
            return false;
        }

        if (error == Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION
                && (batch.sequenceHasBeenReset()
                        || !isNextSequence(tableBucket, batch.batchSequence())
                        || lastAckedBatchSequence(tableBucket)
                                        .orElse(IdempotenceBucketEntry.NO_LAST_ACKED_BATCH_SEQUENCE)
                                > batch.lastAckedSequenceAtSend())) {
            // We should retry the OutOfOrderSequenceException if the batch is not the next batch,
            // i.e. its batch sequence isn't the lastAckedBatchSequence + 1. However, if the first
            // in flight batch fails fatally, we will adjust the batch sequences of the other
            // inflight batches to account for the 'loss' of the sequence range in the batch which
            // failed. In this case, an inflight batch will have a batch sequence which is the
            // lastAckedSequence + 1 after adjustment. When this batch fails with an
            // OutOfOrderSequence, we want to retry it. To account for the latter case, we check
            // whether the sequence has been reset since the last drain. If it has, we will retry it
            // anyway.
            //
            // We should also retry if the acked sequence has advanced since this batch was sent
            // (lastAckedBatchSequence > lastAckedSequenceAtSend). That means a predecessor was
            // acknowledged while this batch was in flight, so this OutOfOrderSequence reflects a
            // superseded (stale) server state that a response reordering surfaced late; a retry
            // will observe the advanced state and succeed. A genuine loss shows no such progress
            // and still resets.
            return true;
        }

        if (error == Errors.UNKNOWN_WRITER_ID_EXCEPTION && batch.sequenceHasBeenReset()) {
            // When the first inflight batch fails due to the truncation case, then the
            // sequences of all the other in flight batches would have been restarted from the
            // beginning. However, when those responses come back from the tablet server, they
            // would also come with an UNKNOWN_WRITER_ID error. In this case, we should not
            // reset the batch sequence to the beginning.
            return true;
        }

        if (batch.hasWriterEpoch()) {
            if (error == Errors.UNSUPPORTED_VERSION) {
                // Either the connection to the leader negotiated a PRODUCE_LOG version that cannot
                // carry V3, in which case the sender asks the leader for its API versions again,
                // or the leader has not applied the cluster feature level that allows V3 yet. The
                // writer only writes V3 once a tablet server reported the level, and the level is
                // never lowered, so the leader accepts the batch once both are settled. The
                // leader rejected the batch before writing it, so it is resent as it is.
                return true;
            }
            if (error == Errors.INVALID_WRITER_EPOCH_EXCEPTION) {
                // The batch was sent before an epoch bump that renumbered it, and the server has
                // already seen the new epoch. The renumbered batch can be retried.
                return batch.sequenceHasBeenReset() || batch.writerEpoch() < writerEpoch;
            }
        }

        if ((error == Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION
                        || error == Errors.UNKNOWN_WRITER_ID_EXCEPTION)
                && restartsSequenceOnGap(batch)) {
            // A real gap, for example because the server expired the writer state. The server
            // rejected the batch before writing it, so restart the sequence of the bucket and
            // retry the batch from sequence 0, as the Kafka idempotent producer does. A batch
            // whose earlier attempt may have been written is not renumbered: it fails and the
            // writer id is reset.
            if (batch.isOutcomeUnknown()) {
                return false;
            }
            requestSequenceRestart(tableBucket, batch.hasWriterEpoch());
            return true;
        }

        return false;
    }

    /**
     * Returns true if the batch has already been committed on the server.
     *
     * <p>If the batch's sequence is less than or equal to {@code lastAckedBatchSequence}, it means
     * a higher-sequence batch has already been acknowledged. This implies the previous batch was
     * also successfully written on the server (otherwise the higher-sequence batch could not have
     * been committed).
     *
     * @param batch the batch to check
     * @param tableBucket the target table-bucket
     * @return true if the batch is already committed on the server
     */
    synchronized boolean isAlreadyCommitted(WriteBatch batch, TableBucket tableBucket) {
        Optional<Integer> lastAcked = lastAckedBatchSequence(tableBucket);
        return lastAcked.isPresent() && batch.batchSequence() <= lastAcked.get();
    }

    void maybeWaitForWriterId(Set<PhysicalTablePath> tablePaths) throws Throwable {
        if (isWriterIdValid()) {
            return;
        }
        setWriterId(requestWriterId(tablePaths));
    }

    /** Requests a new writer id from the server, with a few retries. */
    private long requestWriterId(Set<PhysicalTablePath> tablePaths) throws Throwable {
        int retryCount = 0;
        while (true) {
            try {
                // TODO: can optimize into async response handling.
                return tabletServerGateway
                        .initWriter(prepareInitWriterRequest(tablePaths))
                        .get()
                        .getWriterId();
            } catch (Exception e) {
                Throwable t = ExceptionUtils.stripExecutionException(e);
                if (t instanceof AuthorizationException || retryCount >= RETRY_TIMES) {
                    throw t;
                } else {
                    if (t instanceof InvalidMetadataException) {
                        metadataUpdater.updatePhysicalTableMetadata(tablePaths);
                    }
                    LOG.warn(
                            "Failed to init writer id, the retry count: {}, error message: {}",
                            retryCount,
                            t.getMessage());
                    retryCount++;
                    long delayMs = (long) (RETRY_INTERVAL_MS * Math.pow(2, retryCount));
                    Thread.sleep(delayMs);
                }
            }
        }
    }

    InitWriterRequest prepareInitWriterRequest(Set<PhysicalTablePath> physicalTables) {
        InitWriterRequest initWriterRequest = new InitWriterRequest();
        Set<TablePath> tables =
                physicalTables.stream()
                        .map(PhysicalTablePath::getTablePath)
                        .collect(Collectors.toSet());
        for (TablePath tablePath : tables) {
            initWriterRequest
                    .addTablePath()
                    .setDatabaseName(tablePath.getDatabaseName())
                    .setTableName(tablePath.getTableName());
        }
        return initWriterRequest;
    }

    private int maybeUpdateLastAckedSequence(TableBucket tableBucket, int sequence) {
        return idempotenceBucketMap.maybeUpdateLastAckedSequence(tableBucket, sequence);
    }
}
