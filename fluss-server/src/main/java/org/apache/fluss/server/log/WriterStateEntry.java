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

package org.apache.fluss.server.log;

import org.apache.fluss.record.LogRecordBatch;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_EPOCH;

/**
 * This class represents the state of a specific writer id. The batch sequence number is ordered
 * such that the batch with the lowest batch sequence is at the head of the queue while the highest
 * batch sequence is at the tail of the queue. We will retain at most {@value NUM_BATCHES_TO_RETAIN}
 * batches in the queue. When the queue is at capacity, we remove the first element to make space
 * for the incoming batch.
 */
public class WriterStateEntry {
    private static final int NUM_BATCHES_TO_RETAIN = 5;
    private final long writerId;
    private final Deque<BatchMetadata> batchMetadata = new ArrayDeque<>();

    private short writerEpoch;
    private long lastTimestamp;

    public WriterStateEntry(
            long writerId, long lastTimestamp, @Nullable BatchMetadata firstBatchMetadata) {
        this(writerId, NO_WRITER_EPOCH, lastTimestamp, firstBatchMetadata);
    }

    public WriterStateEntry(
            long writerId,
            short writerEpoch,
            long lastTimestamp,
            @Nullable BatchMetadata firstBatchMetadata) {
        this.writerId = writerId;
        this.writerEpoch = writerEpoch;
        this.lastTimestamp = lastTimestamp;
        if (firstBatchMetadata != null) {
            addBatchMetadata(firstBatchMetadata);
        }
    }

    public static WriterStateEntry empty(long writerId) {
        return new WriterStateEntry(writerId, -1L, null);
    }

    public boolean isEmpty() {
        return batchMetadata.isEmpty();
    }

    public long writerId() {
        return writerId;
    }

    public short writerEpoch() {
        return writerEpoch;
    }

    public int firstBatchSequence() {
        return isEmpty() ? NO_BATCH_SEQUENCE : batchMetadata.getFirst().batchSequence;
    }

    public int lastBatchSequence() {
        return isEmpty() ? NO_BATCH_SEQUENCE : batchMetadata.getLast().batchSequence;
    }

    public int lastBatchFirstSequence() {
        return isEmpty() ? NO_BATCH_SEQUENCE : batchMetadata.getLast().firstSequence;
    }

    public long firstDataOffset() {
        return isEmpty() ? -1L : batchMetadata.getFirst().firstOffset();
    }

    public long lastDataOffset() {
        return isEmpty() ? -1L : batchMetadata.getLast().lastOffset;
    }

    public int lastOffsetDelta() {
        return isEmpty() ? -1 : batchMetadata.getLast().offsetDelta;
    }

    public long lastBatchTimestamp() {
        return lastTimestamp;
    }

    public void addBath(int batchSequence, long lastOffset, int offsetDelta, long timestamp) {
        addBath(batchSequence, batchSequence, lastOffset, offsetDelta, timestamp);
    }

    public void addBath(
            int firstSequence, int lastSequence, long lastOffset, int offsetDelta, long timestamp) {
        addBatchMetadata(
                new BatchMetadata(
                        writerId, firstSequence, lastSequence, lastOffset, offsetDelta, timestamp));
        this.lastTimestamp = timestamp;
    }

    /**
     * Moves this entry to a higher writer epoch. The batches of the previous epoch are dropped, as
     * the writer restarts its sequence from 0.
     */
    public void bumpEpoch(short newEpoch) {
        this.writerEpoch = newEpoch;
        this.batchMetadata.clear();
    }

    public void update(WriterStateEntry nextEntry) {
        if (nextEntry.writerEpoch != writerEpoch) {
            bumpEpoch(nextEntry.writerEpoch);
        }
        update(nextEntry.lastTimestamp, nextEntry.batchMetadata);
    }

    private void update(long lastTimestamp, Deque<BatchMetadata> batchMetadata) {
        while (!batchMetadata.isEmpty()) {
            addBatchMetadata(batchMetadata.removeFirst());
        }
        this.lastTimestamp = lastTimestamp;
    }

    private void addBatchMetadata(BatchMetadata batch) {
        if (batchMetadata.size() == NUM_BATCHES_TO_RETAIN) {
            batchMetadata.removeFirst();
        }
        batchMetadata.add(batch);
    }

    public Optional<BatchMetadata> findDuplicateBatch(LogRecordBatch batch) {
        if (batch.writerEpoch() != writerEpoch) {
            return Optional.empty();
        }
        int firstSequence = batch.batchSequence();
        int lastSequence = batch.lastSequence();
        return batchMetadata.stream()
                .filter(
                        metadata ->
                                metadata.firstSequence == firstSequence
                                        && metadata.batchSequence == lastSequence)
                .findFirst();
    }

    public Optional<BatchMetadata> findDuplicateBatch(int arriveBatchSequence) {
        return batchMetadata.stream()
                .filter(batchMetadata -> batchMetadata.batchSequence == arriveBatchSequence)
                .findFirst();
    }

    /**
     * Returns a new instance with the provided parameters and the values from the current instance
     * otherwise.
     */
    public WriterStateEntry withWriterIdAndBatchMetadata(
            long writerId, @Nullable BatchMetadata batchMetadata) {
        return new WriterStateEntry(writerId, this.writerEpoch, this.lastTimestamp, batchMetadata);
    }

    /** Metadata of a batch. */
    public static final class BatchMetadata {
        public final long writerId;
        // First sequence of the batch, equal to batchSequence unless the sequence counts records.
        public final int firstSequence;
        // Last sequence of the batch.
        public final int batchSequence;
        public final long lastOffset;
        public final int offsetDelta;
        public final long timestamp;

        public BatchMetadata(
                long writerId,
                int batchSequence,
                long lastOffset,
                int offsetDelta,
                long timestamp) {
            this(writerId, batchSequence, batchSequence, lastOffset, offsetDelta, timestamp);
        }

        public BatchMetadata(
                long writerId,
                int firstSequence,
                int batchSequence,
                long lastOffset,
                int offsetDelta,
                long timestamp) {
            this.writerId = writerId;
            this.firstSequence = firstSequence;
            this.batchSequence = batchSequence;
            this.lastOffset = lastOffset;
            this.offsetDelta = offsetDelta;
            this.timestamp = timestamp;
        }

        public long firstOffset() {
            return lastOffset - offsetDelta;
        }
    }
}
