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

package org.apache.fluss.rpc.protocol.feature;

import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.PbClusterFeatureLevel;
import org.apache.fluss.rpc.protocol.ApiKeys;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ApiFeatureContracts} and {@link ClusterFeatureLevels}. */
class ApiFeatureContractsTest {

    private static final ClusterFeatureLevels BASELINE = ClusterFeatureLevels.baseline();
    private static final ClusterFeatureLevels V3 = BASELINE.with(LOG_RECORD_BATCH_VERSION, 3);

    @Test
    void testLevelsPerApiVersion() {
        assertThat(
                        ApiFeatureContracts.maxLevel(
                                ApiKeys.FETCH_LOG, (short) 0, LOG_RECORD_BATCH_VERSION))
                .isEqualTo(2);
        assertThat(
                        ApiFeatureContracts.maxLevel(
                                ApiKeys.FETCH_LOG, (short) 1, LOG_RECORD_BATCH_VERSION))
                .isEqualTo(3);
        assertThat(
                        ApiFeatureContracts.maxLevel(
                                ApiKeys.LIMIT_SCAN, (short) 0, LOG_RECORD_BATCH_VERSION))
                .isEqualTo(2);
        assertThat(
                        ApiFeatureContracts.maxLevel(
                                ApiKeys.LIMIT_SCAN, (short) 1, LOG_RECORD_BATCH_VERSION))
                .isEqualTo(3);
        // APIs without a contract return no log record batches to a reader
        assertThat(
                        ApiFeatureContracts.maxLevel(
                                ApiKeys.PUT_KV, (short) 0, LOG_RECORD_BATCH_VERSION))
                .isEqualTo(Integer.MAX_VALUE);
        // PRODUCE_LOG v2 is the first version that may carry V3 batches
        assertThat(
                        ApiFeatureContracts.maxLevel(
                                ApiKeys.PRODUCE_LOG, (short) 1, LOG_RECORD_BATCH_VERSION))
                .isEqualTo(2);
        assertThat(
                        ApiFeatureContracts.maxLevel(
                                ApiKeys.PRODUCE_LOG, (short) 2, LOG_RECORD_BATCH_VERSION))
                .isEqualTo(3);
    }

    @Test
    void testWriteVersionsAreAdvertisedOnceTheirLevelIsApplied() {
        // before the raise, a tablet server leaves out PRODUCE_LOG v2
        assertThat(ApiFeatureContracts.highestAdvertisedVersion(ApiKeys.PRODUCE_LOG, BASELINE))
                .isEqualTo((short) 1);
        assertThat(ApiFeatureContracts.highestAdvertisedVersion(ApiKeys.PRODUCE_LOG, V3))
                .isEqualTo(ApiKeys.PRODUCE_LOG.highestSupportedVersion);
        // read APIs advertise every version, older readers get converted data
        assertThat(ApiFeatureContracts.highestAdvertisedVersion(ApiKeys.FETCH_LOG, BASELINE))
                .isEqualTo(ApiKeys.FETCH_LOG.highestSupportedVersion);
        assertThat(ApiFeatureContracts.highestAdvertisedVersion(ApiKeys.PUT_KV, BASELINE))
                .isEqualTo(ApiKeys.PUT_KV.highestSupportedVersion);
    }

    @Test
    void testNegotiatedWriteVersionImpliesTheLevel() {
        // a server that advertises PRODUCE_LOG v2 has applied level 3
        assertThat(
                        ApiFeatureContracts.levelImpliedByWriteVersions(
                                LOG_RECORD_BATCH_VERSION,
                                api -> api == ApiKeys.PRODUCE_LOG ? 2 : -1))
                .isEqualTo(3);
        assertThat(
                        ApiFeatureContracts.levelImpliedByWriteVersions(
                                LOG_RECORD_BATCH_VERSION,
                                api -> api == ApiKeys.PRODUCE_LOG ? 1 : -1))
                .isEqualTo(2);
        // nothing negotiated, for example a coordinator, implies the baseline
        assertThat(
                        ApiFeatureContracts.levelImpliedByWriteVersions(
                                LOG_RECORD_BATCH_VERSION, api -> -1))
                .isEqualTo(2);
    }

    @Test
    void testCheckWriteWithVersion() {
        assertThatCode(
                        () ->
                                ApiFeatureContracts.checkWrite(
                                        ApiKeys.PRODUCE_LOG,
                                        (short) 2,
                                        LOG_RECORD_BATCH_VERSION,
                                        3,
                                        V3))
                .doesNotThrowAnyException();
        // the version cannot carry the level, even though the cluster finalized it
        assertThatThrownBy(
                        () ->
                                ApiFeatureContracts.checkWrite(
                                        ApiKeys.PRODUCE_LOG,
                                        (short) 1,
                                        LOG_RECORD_BATCH_VERSION,
                                        3,
                                        V3))
                .isInstanceOf(UnsupportedVersionException.class)
                .hasMessageContaining("PRODUCE_LOG version 1 carries cluster feature");
        // the version can carry the level, but the cluster has not finalized it
        assertThatThrownBy(
                        () ->
                                ApiFeatureContracts.checkWrite(
                                        ApiKeys.PRODUCE_LOG,
                                        (short) 2,
                                        LOG_RECORD_BATCH_VERSION,
                                        3,
                                        BASELINE))
                .isInstanceOf(UnsupportedVersionException.class)
                .hasMessageContaining("the cluster has finalized level 2");
    }

    @Test
    void testCheckWrite() {
        assertThatCode(() -> ApiFeatureContracts.checkWrite(LOG_RECORD_BATCH_VERSION, 3, V3))
                .doesNotThrowAnyException();
        assertThatCode(() -> ApiFeatureContracts.checkWrite(LOG_RECORD_BATCH_VERSION, 2, V3))
                .doesNotThrowAnyException();
        assertThatCode(() -> ApiFeatureContracts.checkWrite(LOG_RECORD_BATCH_VERSION, 2, BASELINE))
                .doesNotThrowAnyException();

        // the cluster has not finalized the level
        assertThatThrownBy(
                        () -> ApiFeatureContracts.checkWrite(LOG_RECORD_BATCH_VERSION, 3, BASELINE))
                .isInstanceOf(UnsupportedVersionException.class)
                .hasMessageContaining("of cluster feature 'log.record-batch.version'")
                .hasMessageContaining("the cluster has finalized level 2");
    }

    @Test
    void testFeatureLevels() {
        assertThat(ClusterFeature.forName("log.record-batch.version"))
                .isSameAs(LOG_RECORD_BATCH_VERSION);
        assertThat(ClusterFeature.forName("no.such-feature")).isNull();
        assertThat(ClusterFeatureLevels.latest().level(LOG_RECORD_BATCH_VERSION)).isEqualTo(3);
        // only features above their baseline are stored for the cluster
        assertThat(BASELINE.toMap()).isEmpty();
        assertThat(V3.toMap()).containsOnlyKeys("log.record-batch.version").containsValue(3);
        assertThat(ClusterFeatureLevels.fromMap(V3.toMap())).isEqualTo(V3);
        assertThat(ClusterFeatureLevels.unknownFeatures(V3.toMap())).isEmpty();
        assertThat(BASELINE.level(LOG_RECORD_BATCH_VERSION)).isEqualTo(2);
        assertThat(V3.level(LOG_RECORD_BATCH_VERSION)).isEqualTo(3);
        // a copy with a new level leaves the original unchanged
        assertThat(BASELINE.level(LOG_RECORD_BATCH_VERSION)).isEqualTo(2);
    }

    @Test
    void testReportedLevelsRoundTrip() {
        ApiVersionsResponse response = new ApiVersionsResponse();
        V3.reportIn(response);

        assertThat(response.getFinalizedFeaturesList())
                .singleElement()
                .satisfies(
                        reported -> {
                            assertThat(reported.getName()).isEqualTo("log.record-batch.version");
                            assertThat(reported.getLevel()).isEqualTo(3);
                        });
        assertThat(ClusterFeatureLevels.fromReported(response.getFinalizedFeaturesList()))
                .isEqualTo(V3);
    }

    @Test
    void testLevelsOfServerThatReportsNoneAreTheBaseline() {
        // a server that does not know cluster features reports none
        assertThat(ClusterFeatureLevels.fromReported(Collections.emptyList())).isEqualTo(BASELINE);
        // a feature this build does not know is ignored
        assertThat(
                        ClusterFeatureLevels.fromReported(
                                Collections.singletonList(reported("some.future-feature", 7))))
                .isEqualTo(BASELINE);
    }

    @Test
    void testReportedLevelIsNeverBelowTheBaseline() {
        assertThat(
                        ClusterFeatureLevels.fromReported(
                                        Arrays.asList(
                                                reported("log.record-batch.version", 1),
                                                reported("some.future-feature", 7)))
                                .level(LOG_RECORD_BATCH_VERSION))
                .isEqualTo(2);
        // a newer server may have finalized a level this build does not know yet
        assertThat(
                        ClusterFeatureLevels.fromReported(
                                        Collections.singletonList(
                                                reported("log.record-batch.version", 4)))
                                .level(LOG_RECORD_BATCH_VERSION))
                .isEqualTo(4);
    }

    private static PbClusterFeatureLevel reported(String name, int level) {
        return new PbClusterFeatureLevel().setName(name).setLevel(level);
    }
}
