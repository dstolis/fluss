/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.feature;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ClusterFeatures}. */
class ClusterFeaturesTest {

    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zkClient;

    @BeforeAll
    static void beforeAll() {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @AfterAll
    static void afterAll() {
        zkClient.close();
    }

    @Test
    void testServerStartsWhileItSupportsTheFinalizedLevels() throws Exception {
        // nothing finalized yet
        assertThatCode(
                        () ->
                                ClusterFeatures.checkSupportsFinalizedFeatures(
                                        new Configuration(), zkClient))
                .doesNotThrowAnyException();

        setFinalizedLevel(3);
        assertThatCode(
                        () ->
                                ClusterFeatures.checkSupportsFinalizedFeatures(
                                        new Configuration(), zkClient))
                .doesNotThrowAnyException();
    }

    @Test
    void testServerRefusesToStartBelowTheFinalizedLevel() throws Exception {
        // a newer version of the cluster finalized a level this server does not know
        setFinalizedLevel(4);

        assertThatThrownBy(
                        () ->
                                ClusterFeatures.checkSupportsFinalizedFeatures(
                                        new Configuration(), zkClient))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(
                        "does not support the finalized cluster features "
                                + "[log.record-batch.version (supports 3, finalized 4)]");
    }

    @Test
    void testFeatureLevelMustNotBeInServerConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        assertThatCode(() -> ClusterFeatures.checkNotInServerConfig(conf))
                .doesNotThrowAnyException();

        // any level, also the baseline, and also a feature this version does not know
        conf.set(ConfigOptions.CLUSTER_FEATURE_LOG_RECORD_BATCH_VERSION, 2);
        conf.setString("cluster.feature.added-by-a-later-version", "1");
        assertThatThrownBy(() -> ClusterFeatures.checkNotInServerConfig(conf))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(
                        "[cluster.feature.added-by-a-later-version, "
                                + "cluster.feature.log.record-batch.version] must not be set in "
                                + "the server configuration");
    }

    @Test
    void testUnsupportedFeatures() {
        assertThat(
                        ClusterFeatures.unsupportedFeatures(
                                Collections.emptyMap(), ClusterFeatureLevels.baseline()))
                .isEmpty();
    }

    private static void setFinalizedLevel(int level) throws Exception {
        zkClient.upsertEntityConfigs(
                Collections.singletonMap(
                        ConfigOptions.CLUSTER_FEATURE_LOG_RECORD_BATCH_VERSION.key(),
                        String.valueOf(level)));
    }
}
