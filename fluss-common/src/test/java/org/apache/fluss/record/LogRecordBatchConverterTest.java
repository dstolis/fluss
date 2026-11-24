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

import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_EPOCH;
import static org.apache.fluss.record.LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsV3;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LogRecordBatchConverter}. */
class LogRecordBatchConverterTest {

    private final TestingSchemaGetter schemaGetter =
            new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA);

    @Test
    void testV3BatchIsConvertedToV0WithTheSameRecords() throws Exception {
        List<Object[]> rows = DATA1.subList(0, 3);
        MemoryLogRecords v3 = genMemoryLogRecordsV3(rows, 7L, (short) 2, 5, true, 10L);
        LogRecordBatch original = v3.batches().iterator().next();

        LogRecords converted = LogRecordBatchConverter.convert(v3, LOG_MAGIC_VALUE_V2);

        List<LogRecordBatch> batches = batches(converted);
        assertThat(batches).hasSize(1);
        LogRecordBatch batch = batches.get(0);
        batch.ensureValid();
        assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V0);
        assertThat(batch.baseLogOffset()).isEqualTo(original.baseLogOffset());
        assertThat(batch.lastLogOffset()).isEqualTo(original.lastLogOffset());
        assertThat(batch.commitTimestamp()).isEqualTo(original.commitTimestamp());
        assertThat(batch.schemaId()).isEqualTo(original.schemaId());
        assertThat(batch.writerId()).isEqualTo(7L);
        assertThat(batch.batchSequence()).isEqualTo(5);
        // the writer epoch and the per-record sequence mode are not part of V0
        assertThat(batch.writerEpoch()).isEqualTo(NO_WRITER_EPOCH);
        assertThat(batch.lastSequence()).isEqualTo(5);
        assertThat(readIndexedRows(converted)).containsExactlyElementsOf(readIndexedRows(v3));
    }

    @Test
    void testArrowBatchWithStatisticsIsConvertedToV0() throws Exception {
        MemoryLogRecords v3 =
                createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID, LOG_MAGIC_VALUE_V3);

        LogRecords converted = LogRecordBatchConverter.convert(v3, LOG_MAGIC_VALUE_V2);

        LogRecordBatch batch = batches(converted).get(0);
        batch.ensureValid();
        assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V0);
        assertThat(readArrowRows(converted)).containsExactlyElementsOf(readArrowRows(v3));
    }

    @Test
    void testReadableBatchesAreCopiedAsStored() throws Exception {
        MemoryLogRecords v2 = genMemoryLogRecordsByObject(LOG_MAGIC_VALUE_V2, DATA1);

        // nothing to convert, the records are returned as they are
        assertThat(LogRecordBatchConverter.convert(v2, LOG_MAGIC_VALUE_V2)).isSameAs(v2);
        assertThat(LogRecordBatchConverter.containsMagicAbove(v2, LOG_MAGIC_VALUE_V2)).isFalse();
    }

    @Test
    void testMixedBatchesKeepTheirOrder() throws Exception {
        MemoryLogRecords v3 =
                genMemoryLogRecordsV3(DATA1.subList(0, 2), 7L, (short) 0, 0, false, 0L);
        MemoryLogRecords v0 = genMemoryLogRecordsByObject(LOG_MAGIC_VALUE_V0, DATA1.subList(2, 4));
        byte[] bytes = new byte[v3.sizeInBytes() + v0.sizeInBytes()];
        v3.getMemorySegment().get(v3.getPosition(), bytes, 0, v3.sizeInBytes());
        v0.getMemorySegment().get(v0.getPosition(), bytes, v3.sizeInBytes(), v0.sizeInBytes());
        MemoryLogRecords mixed = MemoryLogRecords.pointToBytes(bytes);

        LogRecords converted = LogRecordBatchConverter.convert(mixed, LOG_MAGIC_VALUE_V2);

        List<LogRecordBatch> batches = batches(converted);
        assertThat(batches).hasSize(2);
        assertThat(batches.get(0).magic()).isEqualTo(LOG_MAGIC_VALUE_V0);
        assertThat(batches.get(1).magic()).isEqualTo(LOG_MAGIC_VALUE_V0);
        batches.forEach(LogRecordBatch::ensureValid);
        assertThat(LogRecordBatchConverter.containsMagicAbove(converted, LOG_MAGIC_VALUE_V2))
                .isFalse();
    }

    private static List<LogRecordBatch> batches(LogRecords records) {
        List<LogRecordBatch> batches = new ArrayList<>();
        records.batches().forEach(batches::add);
        return batches;
    }

    private List<String> readIndexedRows(LogRecords records) throws Exception {
        try (LogRecordReadContext context =
                LogRecordReadContext.createIndexedReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, schemaGetter)) {
            return readRows(records, context);
        }
    }

    private List<String> readArrowRows(LogRecords records) throws Exception {
        try (LogRecordReadContext context =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, schemaGetter)) {
            return readRows(records, context);
        }
    }

    private static List<String> readRows(LogRecords records, LogRecordReadContext context) {
        List<String> rows = new ArrayList<>();
        for (LogRecordBatch batch : records.batches()) {
            try (CloseableIterator<LogRecord> iterator = batch.records(context)) {
                while (iterator.hasNext()) {
                    LogRecord record = iterator.next();
                    rows.add(
                            record.logOffset()
                                    + ":"
                                    + record.getRow().getInt(0)
                                    + ":"
                                    + record.getRow().getString(1));
                }
            }
        }
        return rows;
    }
}
