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

package org.apache.fluss.server.tablet;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.entity.ProduceLogDataForBucket;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsV3;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the check of {@link TabletService} that a produce request's version can carry its data. */
class ProduceLogVersionCheckTest {

    private static final TableBucket V2_BUCKET = new TableBucket(1L, 0);
    private static final TableBucket V3_BUCKET = new TableBucket(1L, 1);

    @Test
    void testV3BatchesNeedProduceLogVersion2() throws Exception {
        List<ProduceLogDataForBucket> data =
                Arrays.asList(
                        new ProduceLogDataForBucket(
                                V2_BUCKET,
                                genMemoryLogRecordsByObject(LOG_MAGIC_VALUE_V2, DATA1),
                                null),
                        new ProduceLogDataForBucket(
                                V3_BUCKET,
                                genMemoryLogRecordsV3(DATA1, 1L, (short) 0, 0, false, 0L),
                                null));

        // v1 can carry the V2 batches, but not the V3 ones, which are rejected per bucket with
        // an error the writer retries after asking for the API versions again
        Map<TableBucket, ProduceLogResultForBucket> errors = new HashMap<>();
        TabletService.collectUnsupportedFormatErrors(data, (short) 1, errors);
        assertThat(errors).containsOnlyKeys(V3_BUCKET);
        assertThat(errors.get(V3_BUCKET).getError().error()).isEqualTo(Errors.UNSUPPORTED_VERSION);
        assertThat(errors.get(V3_BUCKET).getError().message())
                .contains("PRODUCE_LOG version 1 carries log record batches up to magic 2")
                .contains("magic 3");

        // v2 can carry both
        errors.clear();
        TabletService.collectUnsupportedFormatErrors(data, (short) 2, errors);
        assertThat(errors).isEmpty();
    }
}
