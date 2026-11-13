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
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.V0_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.attributeOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.batchSequenceOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.crcOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.lastOffsetDeltaOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.recordsCountOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.schemaIdOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.statisticsLengthOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.writeClientIdOffset;

/**
 * Converts log record batches for readers that only understand older batch formats.
 *
 * <p>A batch whose magic is above what the reader supports is rewritten as a V0 batch, the only
 * format every released client reads the same way: magic V1 meant a leader epoch before Fluss 1.0
 * and statistics since. The records, offsets, commit timestamp, schema id, writer id and sequence
 * are kept. The writer epoch, leader epoch and statistics are dropped, which readers do not need.
 * Batches the reader supports are copied as stored.
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
     * Returns the records with every batch whose magic is above {@code maxMagic} rewritten as V0.
     * The records are returned as they are if no batch needs to be rewritten.
     */
    public static LogRecords convert(LogRecords records, byte maxMagic) {
        if (!containsMagicAbove(records, maxMagic)) {
            return records;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(records.sizeInBytes());
        for (LogRecordBatch batch : records.batches()) {
            DefaultLogRecordBatch fullBatch = fullBatch(batch);
            if (fullBatch.magic() <= maxMagic) {
                out.write(copy(fullBatch), 0, fullBatch.sizeInBytes());
            } else {
                byte[] converted = toV0(fullBatch);
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

    /** Rewrites one batch as a V0 batch with the same records. */
    static byte[] toV0(DefaultLogRecordBatch batch) {
        MemorySegment src = batch.segment();
        int pos = batch.position();
        byte magic = batch.magic();
        int statisticsLength =
                magic >= LOG_MAGIC_VALUE_V1 ? src.getInt(pos + statisticsLengthOffset(magic)) : 0;
        int recordsStart = recordBatchHeaderSize(magic) + statisticsLength;
        int recordsLength = batch.sizeInBytes() - recordsStart;

        byte v0 = LOG_MAGIC_VALUE_V0;
        byte[] bytes = new byte[V0_RECORD_BATCH_HEADER_SIZE + recordsLength];
        MemorySegment dst = MemorySegment.wrap(bytes);
        dst.putLong(BASE_OFFSET_OFFSET, src.getLong(pos + BASE_OFFSET_OFFSET));
        dst.putInt(LENGTH_OFFSET, bytes.length - LOG_OVERHEAD);
        dst.put(MAGIC_OFFSET, v0);
        dst.putLong(COMMIT_TIMESTAMP_OFFSET, src.getLong(pos + COMMIT_TIMESTAMP_OFFSET));
        dst.putShort(schemaIdOffset(v0), src.getShort(pos + schemaIdOffset(magic)));
        dst.put(attributeOffset(v0), (byte) (batch.attributes() & APPEND_ONLY_FLAG_MASK));
        dst.putInt(lastOffsetDeltaOffset(v0), src.getInt(pos + lastOffsetDeltaOffset(magic)));
        dst.putLong(writeClientIdOffset(v0), src.getLong(pos + writeClientIdOffset(magic)));
        dst.putInt(batchSequenceOffset(v0), src.getInt(pos + batchSequenceOffset(magic)));
        dst.putInt(recordsCountOffset(v0), src.getInt(pos + recordsCountOffset(magic)));
        src.get(pos + recordsStart, bytes, V0_RECORD_BATCH_HEADER_SIZE, recordsLength);

        // the checksum covers everything from the schema id to the end of the batch
        int checksumStart = schemaIdOffset(v0);
        long crc =
                Crc32C.compute(ByteBuffer.wrap(bytes), checksumStart, bytes.length - checksumStart);
        dst.putInt(crcOffset(v0), (int) crc);
        return bytes;
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
