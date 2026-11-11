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

import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_EPOCH;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DefaultLogRecordBatch}. */
public class DefaultLogRecordBatchTest extends LogTestBase {

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V3})
    void testRecordBatchSize(byte magic) throws Exception {
        MemoryLogRecords memoryLogRecords =
                DataTestUtils.genMemoryLogRecordsByObject(magic, TestData.DATA1);
        int totalSize = 0;
        for (LogRecordBatch logRecordBatch : memoryLogRecords.batches()) {
            totalSize += logRecordBatch.sizeInBytes();
        }
        assertThat(totalSize).isEqualTo(memoryLogRecords.sizeInBytes());
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V3})
    void testIndexedRowWriteAndReadBatch(byte magic) throws Exception {
        int recordNumber = 50;
        RowType allRowType = TestInternalRowGenerator.createAllRowType();
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        baseLogOffset,
                        schemaId,
                        Integer.MAX_VALUE,
                        magic,
                        new UnmanagedPagedOutputView(100));

        List<IndexedRow> rows = new ArrayList<>();
        for (int i = 0; i < recordNumber; i++) {
            IndexedRow row = TestInternalRowGenerator.genIndexedRowForAllType();
            builder.append(ChangeType.INSERT, row);
            rows.add(row);
        }

        MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();

        logRecordBatch.ensureValid();

        assertThat(logRecordBatch.getRecordCount()).isEqualTo(recordNumber);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(baseLogOffset);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(baseLogOffset + recordNumber - 1);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(baseLogOffset + recordNumber);
        assertThat(logRecordBatch.magic()).isEqualTo(magic);
        assertThat(logRecordBatch.isValid()).isTrue();
        assertThat(logRecordBatch.schemaId()).isEqualTo(schemaId);

        SchemaGetter schemaGetter = new TestingSchemaGetter(schemaId, allRowType);
        // verify record.
        int i = 0;
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                allRowType, schemaId, schemaGetter);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            while (iter.hasNext()) {
                LogRecord record = iter.next();
                assertThat(record.logOffset()).isEqualTo(i);
                assertThat(record.getChangeType()).isEqualTo(ChangeType.INSERT);
                assertThat(record.getRow()).isEqualTo(rows.get(i));
                i++;
            }
        }

        builder.close();
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V3})
    void testNoRecordAppend(byte magic) throws Exception {
        // 1. no record append with baseOffset as 0.
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        0L, schemaId, Integer.MAX_VALUE, magic, new UnmanagedPagedOutputView(100));
        MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();
        // only contains batch header.
        assertThat(memoryLogRecords.sizeInBytes()).isEqualTo(recordBatchHeaderSize(magic));

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(0);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(0);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(1);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(0);
        SchemaGetter schemaGetter =
                new TestingSchemaGetter(
                        new SchemaInfo(
                                Schema.newBuilder().fromRowType(baseRowType).build(), schemaId));
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                baseRowType, schemaId, schemaGetter);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            assertThat(iter.hasNext()).isFalse();
        }

        // 2. no record append with baseOffset as 100.
        builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        100L,
                        schemaId,
                        Integer.MAX_VALUE,
                        magic,
                        new UnmanagedPagedOutputView(100));
        memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        iterator = memoryLogRecords.batches().iterator();
        // only contains batch header.
        assertThat(memoryLogRecords.sizeInBytes()).isEqualTo(recordBatchHeaderSize(magic));

        assertThat(iterator.hasNext()).isTrue();
        logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(0);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(100);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(101);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(100);

        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(
                                baseRowType, schemaId, schemaGetter);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            assertThat(iter.hasNext()).isFalse();
        }
    }

    @Test
    void testPerRecordSequence() throws Exception {
        LogRecordBatch batch = buildIndexedBatch(LOG_MAGIC_VALUE_V3, 3, (short) 4, true);
        assertThat(batch.writerEpoch()).isEqualTo((short) 4);
        assertThat(batch.batchSequence()).isEqualTo(10);
        // records [10, 11, 12]
        assertThat(batch.lastSequence()).isEqualTo(12);
    }

    @Test
    void testPerBatchSequence() throws Exception {
        LogRecordBatch batch = buildIndexedBatch(LOG_MAGIC_VALUE_V3, 3, (short) 4, false);
        assertThat(batch.writerEpoch()).isEqualTo((short) 4);
        assertThat(batch.batchSequence()).isEqualTo(10);
        assertThat(batch.lastSequence()).isEqualTo(10);
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testPreV3BatchHasNoWriterEpoch(byte magic) throws Exception {
        LogRecordBatch batch = buildIndexedBatch(magic, 3, NO_WRITER_EPOCH, false);
        assertThat(batch.writerEpoch()).isEqualTo(NO_WRITER_EPOCH);
        assertThat(batch.lastSequence()).isEqualTo(batch.batchSequence());

        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        0L, schemaId, Integer.MAX_VALUE, magic, new UnmanagedPagedOutputView(100));
        assertThatThrownBy(() -> builder.setWriterEpochAndSequenceMode((short) 0, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("require magic v3");
    }

    @Test
    void testIncrementSequenceWrapsAround() {
        assertThat(DefaultLogRecordBatch.incrementSequence(5, 3)).isEqualTo(8);
        assertThat(DefaultLogRecordBatch.incrementSequence(Integer.MAX_VALUE, 0))
                .isEqualTo(Integer.MAX_VALUE);
        assertThat(DefaultLogRecordBatch.incrementSequence(Integer.MAX_VALUE, 1)).isEqualTo(0);
        assertThat(DefaultLogRecordBatch.incrementSequence(Integer.MAX_VALUE - 1, 3)).isEqualTo(1);
    }

    private LogRecordBatch buildIndexedBatch(
            byte magic, int recordNumber, short writerEpoch, boolean perRecordSequence)
            throws Exception {
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        0L, schemaId, Integer.MAX_VALUE, magic, new UnmanagedPagedOutputView(100));
        for (int i = 0; i < recordNumber; i++) {
            builder.append(ChangeType.INSERT, TestInternalRowGenerator.genIndexedRowForAllType());
        }
        builder.setWriterState(7L, 10);
        builder.setWriterEpochAndSequenceMode(writerEpoch, perRecordSequence);
        LogRecordBatch batch =
                MemoryLogRecords.pointToBytesView(builder.build()).batches().iterator().next();
        batch.ensureValid();
        assertThat(batch.magic()).isEqualTo(magic);
        assertThat(batch.writerId()).isEqualTo(7L);
        builder.close();
        return batch;
    }
}
