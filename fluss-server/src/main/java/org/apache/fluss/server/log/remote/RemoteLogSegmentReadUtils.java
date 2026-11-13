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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.record.DefaultLogRecordBatch;
import org.apache.fluss.record.LogRecordBatchFormat;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/** Utilities to read log record batches from the data of a remote log segment. */
public final class RemoteLogSegmentReadUtils {

    private RemoteLogSegmentReadUtils() {}

    /**
     * Reads whole batches from a segment stream, starting with the batch that holds {@code
     * fetchOffset}, until adding the next one would exceed {@code maxBytes} or the segment ends.
     * The first batch is always read.
     *
     * <p>{@code startPosition} comes from the offset index, which points at or before the batch
     * that holds the fetch offset. The batches that end before the fetch offset are skipped, and
     * only their headers are read.
     *
     * @param in the data of the segment, positioned at its start
     * @param startPosition the position in the segment to start reading at
     * @param segmentSize the size of the segment in bytes
     * @param fetchOffset the offset the first returned batch must hold
     * @param maxBytes the size the returned batches should not exceed
     * @return the bytes of the batches, empty if no batch holds the fetch offset or follows it
     */
    public static byte[] readBatches(
            InputStream in, int startPosition, int segmentSize, long fetchOffset, int maxBytes)
            throws IOException {
        skipFully(in, startPosition);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] headerUpToMagic = new byte[LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC];
        DefaultLogRecordBatch header = new DefaultLogRecordBatch();
        int position = startPosition;
        while (position + headerUpToMagic.length <= segmentSize) {
            if (!readFully(in, headerUpToMagic, 0, headerUpToMagic.length)) {
                break;
            }
            // the size of the header depends on the magic
            byte magic = headerUpToMagic[LogRecordBatchFormat.MAGIC_OFFSET];
            byte[] headerBytes =
                    Arrays.copyOf(
                            headerUpToMagic, LogRecordBatchFormat.recordBatchHeaderSize(magic));
            if (!readFully(
                    in,
                    headerBytes,
                    headerUpToMagic.length,
                    headerBytes.length - headerUpToMagic.length)) {
                break;
            }
            header.pointTo(MemorySegment.wrap(headerBytes), 0);
            int batchSize = header.sizeInBytes();
            int recordsSize = batchSize - headerBytes.length;
            position += batchSize;
            if (out.size() == 0 && header.lastLogOffset() < fetchOffset) {
                skipFully(in, recordsSize);
                continue;
            }
            if (out.size() > 0 && out.size() + batchSize > maxBytes) {
                break;
            }
            byte[] records = new byte[recordsSize];
            if (!readFully(in, records, 0, records.length)) {
                break;
            }
            out.write(headerBytes, 0, headerBytes.length);
            out.write(records, 0, records.length);
        }
        return out.toByteArray();
    }

    private static void skipFully(InputStream in, long bytes) throws IOException {
        long remaining = bytes;
        while (remaining > 0) {
            long skipped = in.skip(remaining);
            if (skipped <= 0) {
                if (in.read() < 0) {
                    throw new EOFException("Reached the end of the segment while skipping.");
                }
                skipped = 1;
            }
            remaining -= skipped;
        }
    }

    /** Fills {@code length} bytes of the buffer from {@code offset}, false if the stream ends. */
    private static boolean readFully(InputStream in, byte[] buffer, int offset, int length)
            throws IOException {
        int read = 0;
        while (read < length) {
            int n = in.read(buffer, offset + read, length - read);
            if (n < 0) {
                return false;
            }
            read += n;
        }
        return true;
    }
}
