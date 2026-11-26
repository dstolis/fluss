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

import org.apache.fluss.server.log.WriterStateEntry.BatchMetadata;
import org.apache.fluss.server.log.WriterStateManager.WriterSnapshotEntry;
import org.apache.fluss.server.log.WriterStateManager.WriterSnapshotMap;
import org.apache.fluss.utils.json.JsonSerdeTestBase;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.fluss.server.log.WriterStateManager.WriterSnapshotMapJsonSerde}. */
public class WriterSnapshotMapJsonSerdeTest extends JsonSerdeTestBase<WriterSnapshotMap> {

    public WriterSnapshotMapJsonSerdeTest() {
        super(WriterStateManager.WriterSnapshotMapJsonSerde.INSTANCE);
    }

    @Override
    protected WriterSnapshotMap[] createObjects() {
        List<WriterSnapshotEntry> entries =
                Arrays.asList(
                        entry(1001, (short) 0, 23, 23, 100, 1000, 2000),
                        entry(1001, (short) 0, 25, 25, 200, 3000, 4000),
                        entry(1002, (short) 0, 33, 33, 300, 4000, 5000),
                        entry(1003, (short) 2, 10, 14, 400, 4, 6000),
                        new WriterSnapshotEntry(
                                1004,
                                (short) 1,
                                5,
                                6,
                                501,
                                1,
                                7000,
                                Arrays.asList(
                                        new BatchMetadata(1004, 0, 1, 497, 1, 6800),
                                        new BatchMetadata(1004, 2, 4, 500, 2, 6900))));
        WriterSnapshotMap map = new WriterSnapshotMap(entries);
        return new WriterSnapshotMap[] {map};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":2,\"writer_id_entries\":["
                    + "{\"writer_id\":1001,\"last_batch_sequence\":23,\"last_batch_base_offset\":100,\"offset_delta\":1000,\"last_batch_timestamp\":2000,\"writer_epoch\":0,\"last_batch_first_sequence\":23},"
                    + "{\"writer_id\":1001,\"last_batch_sequence\":25,\"last_batch_base_offset\":200,\"offset_delta\":3000,\"last_batch_timestamp\":4000,\"writer_epoch\":0,\"last_batch_first_sequence\":25},"
                    + "{\"writer_id\":1002,\"last_batch_sequence\":33,\"last_batch_base_offset\":300,\"offset_delta\":4000,\"last_batch_timestamp\":5000,\"writer_epoch\":0,\"last_batch_first_sequence\":33},"
                    + "{\"writer_id\":1003,\"last_batch_sequence\":14,\"last_batch_base_offset\":400,\"offset_delta\":4,\"last_batch_timestamp\":6000,\"writer_epoch\":2,\"last_batch_first_sequence\":10},"
                    + "{\"writer_id\":1004,\"last_batch_sequence\":6,\"last_batch_base_offset\":501,\"offset_delta\":1,\"last_batch_timestamp\":7000,\"writer_epoch\":1,\"last_batch_first_sequence\":5,"
                    + "\"earlier_batches\":[{\"first_sequence\":0,\"last_sequence\":1,\"last_offset\":497,\"offset_delta\":1,\"timestamp\":6800},"
                    + "{\"first_sequence\":2,\"last_sequence\":4,\"last_offset\":500,\"offset_delta\":2,\"timestamp\":6900}]}]}"
        };
    }

    @Test
    void testDeserializeVersion1() {
        String json =
                "{\"version\":1,\"writer_id_entries\":["
                        + "{\"writer_id\":1001,\"last_batch_sequence\":23,\"last_batch_base_offset\":100,\"offset_delta\":1000,\"last_batch_timestamp\":2000}]}";
        WriterSnapshotMap map =
                JsonSerdeUtils.readValue(
                        json.getBytes(StandardCharsets.UTF_8),
                        WriterStateManager.WriterSnapshotMapJsonSerde.INSTANCE);
        // a snapshot without writer epochs reads as epoch 0 and a single-sequence last batch
        assertThat(map)
                .isEqualTo(
                        new WriterSnapshotMap(
                                Collections.singletonList(
                                        entry(1001, (short) 0, 23, 23, 100, 1000, 2000))));
    }

    /** A snapshot entry without batches before the last one. */
    private static WriterSnapshotEntry entry(
            long writerId,
            short writerEpoch,
            int lastBatchFirstSequence,
            int lastBatchSequence,
            long lastBatchBaseOffset,
            int lastBatchOffsetDelta,
            long lastBatchTimestamp) {
        return new WriterSnapshotEntry(
                writerId,
                writerEpoch,
                lastBatchFirstSequence,
                lastBatchSequence,
                lastBatchBaseOffset,
                lastBatchOffsetDelta,
                lastBatchTimestamp,
                Collections.emptyList());
    }
}
