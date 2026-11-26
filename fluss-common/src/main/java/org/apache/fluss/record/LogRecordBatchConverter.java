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

package org.apache.fluss.record;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.utils.crc.Crc32C;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.apache.fluss.record.LogRecordBatchFormat.BASE_OFFSET_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.COMMIT_TIMESTAMP_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.V2_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.attributeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.batchSequenceOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.crcOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.lastOffsetDeltaOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.leaderEpochOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordsCountOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.statisticsDataOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.statisticsLengthOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.writeClientIdOffset;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Converts log record batches for readers that only understand older batch formats.
 *
 * <p>A batch whose magic is above what the reader supports is rewritten as a V2 batch, the newest
 * format every reader since Fluss 1.0 decodes. Every reader version reads at least V2, see the read
 * contracts of the RPC layer. The records, offsets, commit timestamp, leader epoch, schema id,
 * writer id, sequence and statistics are kept. Only the writer epoch and the per-record sequence
 * mode are dropped, which readers do not use. Batches the reader supports are copied as stored.
 *
 * <p>Only readers get converted batches. Replicas always copy the original batches, which keep the
 * writer state the leader validated.
 */
@Internal
public final class LogRecordBatchConverter {

    private static final byte APPEND_ONLY_FLAG_MASK = DefaultLogRecordBatch.APPEND_ONLY_FLAG_MASK;

    private LogRecordBatchConverter() {}

    /** Whether any batch of the records has a magic above the given one. */
    public static boolean containsMagicAbove(LogRecords records, byte maxMagic) {
        for (LogRecordBatch batch : records.batches()) {
            if (batch.magic() > maxMagic) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the records with every batch whose magic is above {@code maxMagic} rewritten as V2.
     * The records are returned as they are if no batch needs to be rewritten.
     */
    public static LogRecords convert(LogRecords records, byte maxMagic) {
        if (!containsMagicAbove(records, maxMagic)) {
            return records;
        }
        checkReaderReadsV2(maxMagic);
        ByteArrayOutputStream out = new ByteArrayOutputStream(records.sizeInBytes());
        for (LogRecordBatch batch : records.batches()) {
            DefaultLogRecordBatch fullBatch = fullBatch(batch);
            if (fullBatch.magic() <= maxMagic) {
                out.write(copy(fullBatch), 0, fullBatch.sizeInBytes());
            } else {
                byte[] converted = toV2(fullBatch);
                out.write(converted, 0, converted.length);
            }
        }
        return MemoryLogRecords.pointToBytes(out.toByteArray());
    }

    /** Copies the records into memory, for records backed by a file that is about to be closed. */
    public static MemoryLogRecords copyToMemory(LogRecords records) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(records.sizeInBytes());
        for (LogRecordBatch batch : records.batches()) {
            DefaultLogRecordBatch fullBatch = fullBatch(batch);
            out.write(copy(fullBatch), 0, fullBatch.sizeInBytes());
        }
        return MemoryLogRecords.pointToBytes(out.toByteArray());
    }

    /**
     * Returns the bytes of one complete batch, rewritten as V2 if its magic is above {@code
     * maxMagic}, otherwise as they are.
     */
    public static byte[] convertBatch(byte[] batchBytes, byte maxMagic) {
        DefaultLogRecordBatch batch = new DefaultLogRecordBatch();
        batch.pointTo(MemorySegment.wrap(batchBytes), 0);
        if (batch.magic() <= maxMagic) {
            return batchBytes;
        }
        checkReaderReadsV2(maxMagic);
        return toV2(batch);
    }

    /**
     * Rewrites one batch above V2 as a V2 batch with the same records and statistics. V2 has every
     * field of V3 except the writer epoch, and no per-record sequence mode.
     */
    static byte[] toV2(DefaultLogRecordBatch batch) {
        MemorySegment src = batch.segment();
        int pos = batch.position();
        byte magic = batch.magic();
        // the statistics, if any, are followed by the records, and both are copied as they are
        int bodyStart = statisticsDataOffset(magic);
        int bodyLength = batch.sizeInBytes() - bodyStart;

        byte v2 = LOG_MAGIC_VALUE_V2;
        byte[] bytes = new byte[V2_RECORD_BATCH_HEADER_SIZE + bodyLength];
        MemorySegment dst = MemorySegment.wrap(bytes);
        dst.putLong(BASE_OFFSET_OFFSET, src.getLong(pos + BASE_OFFSET_OFFSET));
        dst.putInt(LENGTH_OFFSET, bytes.length - LOG_OVERHEAD);
        dst.put(MAGIC_OFFSET, v2);
        dst.putLong(COMMIT_TIMESTAMP_OFFSET, src.getLong(pos + COMMIT_TIMESTAMP_OFFSET));
        dst.putInt(leaderEpochOffset(v2), src.getInt(pos + leaderEpochOffset(magic)));
        dst.putShort(schemaIdOffset(v2), src.getShort(pos + schemaIdOffset(magic)));
        dst.put(attributeOffset(v2), (byte) (batch.attributes() & APPEND_ONLY_FLAG_MASK));
        dst.putInt(lastOffsetDeltaOffset(v2), src.getInt(pos + lastOffsetDeltaOffset(magic)));
        dst.putLong(writeClientIdOffset(v2), src.getLong(pos + writeClientIdOffset(magic)));
        dst.putInt(batchSequenceOffset(v2), src.getInt(pos + batchSequenceOffset(magic)));
        dst.putInt(recordsCountOffset(v2), src.getInt(pos + recordsCountOffset(magic)));
        dst.putInt(statisticsLengthOffset(v2), src.getInt(pos + statisticsLengthOffset(magic)));
        src.get(pos + bodyStart, bytes, V2_RECORD_BATCH_HEADER_SIZE, bodyLength);

        // the checksum covers everything from the schema id to the end of the batch
        int checksumStart = schemaIdOffset(v2);
        long crc =
                Crc32C.compute(ByteBuffer.wrap(bytes), checksumStart, bytes.length - checksumStart);
        dst.putInt(crcOffset(v2), (int) crc);
        return bytes;
    }

    private static void checkReaderReadsV2(byte maxMagic) {
        checkArgument(
                maxMagic >= LOG_MAGIC_VALUE_V2,
                "Batches are converted to magic V2, but the reader reads only up to magic %s.",
                maxMagic);
    }

    private static byte[] copy(DefaultLogRecordBatch batch) {
        byte[] bytes = new byte[batch.sizeInBytes()];
        batch.segment().get(batch.position(), bytes, 0, bytes.length);
        return bytes;
    }

    private static DefaultLogRecordBatch fullBatch(LogRecordBatch batch) {
        if (batch instanceof DefaultLogRecordBatch) {
            return (DefaultLogRecordBatch) batch;
        }
        if (batch instanceof FileLogInputStream.FileChannelLogRecordBatch) {
            return (DefaultLogRecordBatch)
                    ((FileLogInputStream.FileChannelLogRecordBatch) batch).loadFullBatch();
        }
        throw new IllegalArgumentException(
                "Unsupported log record batch type " + batch.getClass().getName());
    }
}
