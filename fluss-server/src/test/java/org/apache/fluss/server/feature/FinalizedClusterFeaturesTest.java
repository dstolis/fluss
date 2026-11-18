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

package org.apache.fluss.server.feature;

import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsV3;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FinalizedClusterFeatures}. */
class FinalizedClusterFeaturesTest {

    @Test
    void testWriteOfUnfinalizedFormatIsRejectedUntilTheLevelIsApplied() throws Exception {
        FinalizedClusterFeatures features = new FinalizedClusterFeatures();
        assertThat(features.levels()).isEqualTo(ClusterFeatureLevels.baseline());

        // the formats of the baseline are always accepted
        assertThat(features.checkWrite(genMemoryLogRecordsByObject(LOG_MAGIC_VALUE_V2, DATA1)))
                .isNull();
        // a V3 batch is rejected with an error the writer can retry
        ApiError error =
                features.checkWrite(genMemoryLogRecordsV3(DATA1, 1L, (short) 0, 0, false, 0L));
        assertThat(error).isNotNull();
        assertThat(error.error()).isEqualTo(Errors.UNSUPPORTED_VERSION);
        assertThat(error.message())
                .contains("of cluster feature 'log.record-batch.version'")
                .contains("the cluster has finalized level 2");

        features.apply(ClusterFeatureLevels.latest());
        assertThat(features.checkWrite(genMemoryLogRecordsV3(DATA1, 1L, (short) 0, 0, false, 0L)))
                .isNull();
        assertThat(features.checkWrite(genMemoryLogRecordsByObject(LOG_MAGIC_VALUE_V2, DATA1)))
                .isNull();
    }

    @Test
    void testAppliedLevelIsNeverLowered() {
        FinalizedClusterFeatures features = new FinalizedClusterFeatures();
        features.apply(ClusterFeatureLevels.latest());

        // a lower level than the applied one must never take effect
        features.apply(ClusterFeatureLevels.baseline());
        assertThat(features.levels().level(LOG_RECORD_BATCH_VERSION)).isEqualTo(3);
    }

    @Test
    void testRejectionReadsTheStoredLevelsFirst() throws Exception {
        ManualClock clock = new ManualClock(1000L);
        FinalizedClusterFeatures features = new FinalizedClusterFeatures(clock);
        Map<String, Integer> stored = new HashMap<>();
        AtomicInteger reads = new AtomicInteger();
        features.setStoredLevelsReader(
                () -> {
                    reads.incrementAndGet();
                    return Optional.of(new HashMap<>(stored));
                });

        // the stored level is the baseline, so the V3 batch is rejected after one read
        assertThat(features.checkWrite(v3Records())).isNotNull();
        assertThat(reads).hasValue(1);

        // the cluster raises the level, but the watch has not applied it yet
        stored.put(LOG_RECORD_BATCH_VERSION.featureName(), 3);
        // within the refresh interval the stored levels are not read again
        assertThat(features.checkWrite(v3Records())).isNotNull();
        assertThat(reads).hasValue(1);

        clock.advanceTime(FinalizedClusterFeatures.REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(features.checkWrite(v3Records())).isNull();
        assertThat(reads).hasValue(2);
        assertThat(features.levels().level(LOG_RECORD_BATCH_VERSION)).isEqualTo(3);

        // accepted writes never read the stored levels
        clock.advanceTime(FinalizedClusterFeatures.REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(features.checkWrite(v3Records())).isNull();
        assertThat(reads).hasValue(2);
    }

    @Test
    void testIsFinalizedAtLeastReadsTheStoredLevels() {
        ManualClock clock = new ManualClock(1000L);
        FinalizedClusterFeatures features = new FinalizedClusterFeatures(clock);
        assertThat(features.isFinalizedAtLeast(LOG_RECORD_BATCH_VERSION, LOG_MAGIC_VALUE_V3))
                .isFalse();

        features.setStoredLevelsReader(
                () ->
                        Optional.of(
                                Collections.singletonMap(
                                        LOG_RECORD_BATCH_VERSION.featureName(), 3)));
        assertThat(features.isFinalizedAtLeast(LOG_RECORD_BATCH_VERSION, LOG_MAGIC_VALUE_V3))
                .isTrue();
        assertThat(features.levels().level(LOG_RECORD_BATCH_VERSION)).isEqualTo(3);
    }

    @Test
    void testStoredLevelsThatCannotBeAppliedKeepTheRejection() throws Exception {
        ManualClock clock = new ManualClock(1000L);
        FinalizedClusterFeatures features = new FinalizedClusterFeatures(clock);

        // a level this build does not support is left to the watch, which stops the server
        features.setStoredLevelsReader(
                () ->
                        Optional.of(
                                Collections.singletonMap(
                                        LOG_RECORD_BATCH_VERSION.featureName(), 4)));
        assertThat(features.checkWrite(v3Records())).isNotNull();
        assertThat(features.levels()).isEqualTo(ClusterFeatureLevels.baseline());

        // a failed read keeps the applied levels
        clock.advanceTime(FinalizedClusterFeatures.REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        features.setStoredLevelsReader(
                () -> {
                    throw new Exception("ZooKeeper is not reachable");
                });
        assertThat(features.checkWrite(v3Records())).isNotNull();
        assertThat(features.levels()).isEqualTo(ClusterFeatureLevels.baseline());

        // a cluster without stored levels is at the baselines
        clock.advanceTime(FinalizedClusterFeatures.REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        features.setStoredLevelsReader(Optional::empty);
        assertThat(features.checkWrite(v3Records())).isNotNull();
    }

    private static MemoryLogRecords v3Records() throws Exception {
        return genMemoryLogRecordsV3(DATA1, 1L, (short) 0, 0, false, 0L);
    }
}
