/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.IdempotentBatch;
import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.server.kv.wal.WalBuilder;

import java.io.IOException;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;

/**
 * Builds the Arrow log record batch that stores the records an idempotent Kafka producer sent for
 * one partition.
 *
 * <p>The batch has magic V3, the first format that carries a writer epoch and can number its
 * records individually. The Kafka producer ID and epoch become the writer ID and epoch, and the
 * sequence of the first Kafka record becomes the batch sequence. Every Kafka record is one row, so
 * the last sequence the tablet server derives from the batch is the sequence of the last Kafka
 * record.
 */
@Internal
final class IdempotentArrowLogBuilder implements WalBuilder {

    private final MemorySegmentPool memorySegmentPool;
    private final ManagedPagedOutputView outputView;
    private final MemoryLogRecordsArrowBuilder recordsBuilder;

    IdempotentArrowLogBuilder(
            int schemaId,
            ArrowWriter writer,
            MemorySegmentPool memorySegmentPool,
            IdempotentBatch idempotentBatch)
            throws IOException {
        this.memorySegmentPool = memorySegmentPool;
        this.outputView = new ManagedPagedOutputView(memorySegmentPool);
        this.recordsBuilder =
                MemoryLogRecordsArrowBuilder.builder(
                        schemaId, LOG_MAGIC_VALUE_V3, writer, outputView, true, null);
        recordsBuilder.setWriterState(idempotentBatch.producerId(), idempotentBatch.baseSequence());
        recordsBuilder.setWriterEpochAndSequenceMode(idempotentBatch.producerEpoch(), true);
    }

    @Override
    public void append(ChangeType changeType, InternalRow row) throws Exception {
        recordsBuilder.append(changeType, row);
    }

    @Override
    public MemoryLogRecords build() throws Exception {
        recordsBuilder.close();
        MultiBytesView bytesView = recordsBuilder.build();
        return MemoryLogRecords.pointToByteBuffer(bytesView.getByteBuf().nioBuffer());
    }

    @Override
    public void setWriterState(long writerId, int batchSequence) {
        recordsBuilder.setWriterState(writerId, batchSequence);
    }

    @Override
    public void deallocate() {
        recordsBuilder.recycleArrowWriter();
        memorySegmentPool.returnAll(outputView.allocatedPooledSegments());
    }
}
