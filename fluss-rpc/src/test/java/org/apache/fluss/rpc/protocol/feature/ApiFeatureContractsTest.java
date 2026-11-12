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
import org.apache.fluss.rpc.messages.PbApiVersion;
import org.apache.fluss.rpc.protocol.ApiKeys;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ApiFeatureContracts}. */
class ApiFeatureContractsTest {

    private static final ClusterFeatureLevels BASELINE = ClusterFeatureLevels.baseline();
    private static final ClusterFeatureLevels V3 = BASELINE.with(LOG_RECORD_BATCH_VERSION, 3);

    @Test
    void testLevelsPerApiVersion() {
        assertThat(
                        ApiFeatureContracts.maxLevel(
                                ApiKeys.PRODUCE_LOG, (short) 0, LOG_RECORD_BATCH_VERSION))
                .isEqualTo(2);
        assertThat(
                        ApiFeatureContracts.maxLevel(
                                ApiKeys.PRODUCE_LOG, (short) 1, LOG_RECORD_BATCH_VERSION))
                .isEqualTo(2);
        assertThat(
                        ApiFeatureContracts.maxLevel(
                                ApiKeys.PRODUCE_LOG, (short) 2, LOG_RECORD_BATCH_VERSION))
                .isEqualTo(3);
        assertThat(
                        ApiFeatureContracts.maxLevel(
                                ApiKeys.FETCH_LOG, (short) 0, LOG_RECORD_BATCH_VERSION))
                .isEqualTo(2);
        assertThat(
                        ApiFeatureContracts.maxLevel(
                                ApiKeys.FETCH_LOG, (short) 1, LOG_RECORD_BATCH_VERSION))
                .isEqualTo(3);
        // APIs without a contract carry no log record batches
        assertThat(
                        ApiFeatureContracts.maxLevel(
                                ApiKeys.PUT_KV, (short) 0, LOG_RECORD_BATCH_VERSION))
                .isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testWriteVersionsAreAdvertisedOnlyOnceFinalized() {
        assertThat(ApiFeatureContracts.highestEnabledVersion(ApiKeys.PRODUCE_LOG, BASELINE))
                .isEqualTo((short) 1);
        assertThat(ApiFeatureContracts.highestEnabledVersion(ApiKeys.PRODUCE_LOG, V3))
                .isEqualTo((short) 2);
        // readers may always use the newest version, it returns older data as stored
        assertThat(ApiFeatureContracts.highestEnabledVersion(ApiKeys.FETCH_LOG, BASELINE))
                .isEqualTo((short) 1);
        assertThat(ApiFeatureContracts.highestEnabledVersion(ApiKeys.PUT_KV, BASELINE))
                .isEqualTo(ApiKeys.PUT_KV.highestSupportedVersion);
    }

    @Test
    void testCapAdvertisedVersions() {
        List<PbApiVersion> versions =
                Arrays.asList(
                        apiVersion(ApiKeys.PRODUCE_LOG),
                        apiVersion(ApiKeys.FETCH_LOG),
                        apiVersion(ApiKeys.PUT_KV));

        ApiFeatureContracts.capAdvertisedVersions(versions, BASELINE);

        assertThat(versions.get(0).getMaxVersion()).isEqualTo((short) 1);
        assertThat(versions.get(1).getMaxVersion()).isEqualTo((short) 1);
        assertThat(versions.get(2).getMaxVersion())
                .isEqualTo(ApiKeys.PUT_KV.highestSupportedVersion);
    }

    @Test
    void testCheckWrite() {
        assertThatCode(
                        () ->
                                ApiFeatureContracts.checkWrite(
                                        ApiKeys.PRODUCE_LOG,
                                        (short) 2,
                                        LOG_RECORD_BATCH_VERSION,
                                        3,
                                        V3))
                .doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                ApiFeatureContracts.checkWrite(
                                        ApiKeys.PRODUCE_LOG,
                                        (short) 1,
                                        LOG_RECORD_BATCH_VERSION,
                                        2,
                                        BASELINE))
                .doesNotThrowAnyException();

        // the request version cannot carry the level
        assertThatThrownBy(
                        () ->
                                ApiFeatureContracts.checkWrite(
                                        ApiKeys.PRODUCE_LOG,
                                        (short) 1,
                                        LOG_RECORD_BATCH_VERSION,
                                        3,
                                        V3))
                .isInstanceOf(UnsupportedVersionException.class)
                .hasMessageContaining(
                        "PRODUCE_LOG version 1 carries cluster feature 'log.record-batch.version' up to level 2");
        // the cluster has not finalized the level
        assertThatThrownBy(
                        () ->
                                ApiFeatureContracts.checkWrite(
                                        ApiKeys.PRODUCE_LOG,
                                        (short) 2,
                                        LOG_RECORD_BATCH_VERSION,
                                        3,
                                        BASELINE))
                .isInstanceOf(UnsupportedVersionException.class)
                .hasMessageContaining("the cluster has finalized level 2")
                .hasMessageContaining("cluster.feature.log.record-batch.version");
    }

    @Test
    void testCanRead() {
        assertThat(
                        ApiFeatureContracts.canRead(
                                ApiKeys.FETCH_LOG, (short) 0, LOG_RECORD_BATCH_VERSION, 2))
                .isTrue();
        assertThat(
                        ApiFeatureContracts.canRead(
                                ApiKeys.FETCH_LOG, (short) 0, LOG_RECORD_BATCH_VERSION, 3))
                .isFalse();
        assertThat(
                        ApiFeatureContracts.canRead(
                                ApiKeys.FETCH_LOG, (short) 1, LOG_RECORD_BATCH_VERSION, 3))
                .isTrue();
    }

    @Test
    void testFeatureLevels() {
        assertThat(LOG_RECORD_BATCH_VERSION.configOption().key())
                .isEqualTo(ClusterFeature.CONFIG_PREFIX + LOG_RECORD_BATCH_VERSION.featureName());
        assertThat(BASELINE.level(LOG_RECORD_BATCH_VERSION)).isEqualTo(2);
        assertThat(V3.level(LOG_RECORD_BATCH_VERSION)).isEqualTo(3);
        // a copy with a new level leaves the original unchanged
        assertThat(BASELINE.level(LOG_RECORD_BATCH_VERSION)).isEqualTo(2);
    }

    private static PbApiVersion apiVersion(ApiKeys api) {
        return new PbApiVersion()
                .setApiKey(api.id)
                .setMinVersion(api.lowestSupportedVersion)
                .setMaxVersion(api.highestSupportedVersion);
    }
}
