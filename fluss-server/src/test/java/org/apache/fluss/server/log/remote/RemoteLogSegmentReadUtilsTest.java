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

import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.MemoryLogRecords;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsV3;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteLogSegmentReadUtils}. */
class RemoteLogSegmentReadUtilsTest {

    private static final List<Object[]> RECORDS = DATA1.subList(0, 3);
    private static final int BATCHES = 4;

    /** Four V3 batches of three records each: offsets 0-2, 3-5, 6-8 and 9-11. */
    private byte[] segment;

    private int batchSize;

    @BeforeEach
    void setup() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < BATCHES; i++) {
            byte[] batch =
                    toBytes(
                            genMemoryLogRecordsV3(
                                    RECORDS, 1L, (short) 0, i, false, (long) i * RECORDS.size()));
            batchSize = batch.length;
            out.write(batch, 0, batch.length);
        }
        segment = out.toByteArray();
    }

    @Test
    void testStartsWithBatchOfFetchOffsetWhenPositionIsBeforeIt() throws Exception {
        // the index points at the first batch, the fetch offset is the base offset of the third
        assertThat(baseOffsets(read(0, 6L, Integer.MAX_VALUE))).containsExactly(6L, 9L);
        // and at the second batch
        assertThat(baseOffsets(read(batchSize, 6L, Integer.MAX_VALUE))).containsExactly(6L, 9L);
    }

    @Test
    void testFetchOffsetInsideBatchReturnsThatBatch() throws Exception {
        assertThat(baseOffsets(read(0, 4L, Integer.MAX_VALUE))).containsExactly(3L, 6L, 9L);
        assertThat(baseOffsets(read(0, 5L, Integer.MAX_VALUE))).containsExactly(3L, 6L, 9L);
    }

    @Test
    void testReadsFromPositionWhenItIsTheBatchOfFetchOffset() throws Exception {
        assertThat(baseOffsets(read(0, 0L, Integer.MAX_VALUE))).containsExactly(0L, 3L, 6L, 9L);
        assertThat(baseOffsets(read(2 * batchSize, 6L, Integer.MAX_VALUE))).containsExactly(6L, 9L);
    }

    @Test
    void testMaxBytesLimitsBatchesAfterTheFirst() throws Exception {
        // the first batch is always read, even if it is larger than the limit
        assertThat(baseOffsets(read(0, 3L, 1))).containsExactly(3L);
        assertThat(baseOffsets(read(0, 3L, batchSize))).containsExactly(3L);
        // skipped batches do not count towards the limit
        assertThat(baseOffsets(read(0, 6L, 2 * batchSize))).containsExactly(6L, 9L);
        assertThat(baseOffsets(read(0, 3L, 2 * batchSize + 1))).containsExactly(3L, 6L);
    }

    @Test
    void testFetchOffsetAfterLastBatchReturnsNothing() throws Exception {
        assertThat(read(0, 12L, Integer.MAX_VALUE)).isEmpty();
    }

    @Test
    void testTruncatedLastBatchIsLeftOut() throws Exception {
        byte[] truncated = Arrays.copyOf(segment, segment.length - 1);
        byte[] read =
                RemoteLogSegmentReadUtils.readBatches(
                        new ByteArrayInputStream(truncated),
                        0,
                        truncated.length,
                        6L,
                        Integer.MAX_VALUE);
        assertThat(baseOffsets(read)).containsExactly(6L);
    }

    @Test
    void testSkipsAndReadsBatchesOfDifferentMagic() throws Exception {
        // a V0 batch with offsets 0-9 followed by a V3 batch with offsets 10-12
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] v0 = toBytes(genMemoryLogRecordsByObject(DATA1));
        out.write(v0, 0, v0.length);
        byte[] v3 = toBytes(genMemoryLogRecordsV3(RECORDS, 1L, (short) 0, 0, false, DATA1.size()));
        out.write(v3, 0, v3.length);
        byte[] mixed = out.toByteArray();

        assertThat(
                        baseOffsets(
                                RemoteLogSegmentReadUtils.readBatches(
                                        new ByteArrayInputStream(mixed),
                                        0,
                                        mixed.length,
                                        DATA1.size(),
                                        Integer.MAX_VALUE)))
                .containsExactly((long) DATA1.size());
        assertThat(
                        baseOffsets(
                                RemoteLogSegmentReadUtils.readBatches(
                                        new ByteArrayInputStream(mixed),
                                        0,
                                        mixed.length,
                                        0L,
                                        Integer.MAX_VALUE)))
                .containsExactly(0L, (long) DATA1.size());
    }

    private byte[] read(int startPosition, long fetchOffset, int maxBytes) throws Exception {
        return RemoteLogSegmentReadUtils.readBatches(
                new ByteArrayInputStream(segment),
                startPosition,
                segment.length,
                fetchOffset,
                maxBytes);
    }

    /** The base offsets of the batches, which are also checked to be complete and valid. */
    private static List<Long> baseOffsets(byte[] batches) {
        List<Long> baseOffsets = new ArrayList<>();
        for (LogRecordBatch batch : MemoryLogRecords.pointToBytes(batches).batches()) {
            batch.ensureValid();
            baseOffsets.add(batch.baseLogOffset());
        }
        return baseOffsets;
    }

    private static byte[] toBytes(MemoryLogRecords records) {
        byte[] bytes = new byte[records.sizeInBytes()];
        records.getMemorySegment().get(records.getPosition(), bytes);
        return bytes;
    }
}
