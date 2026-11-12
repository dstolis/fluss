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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ClusterFeatureLevelValidator}. */
class ClusterFeatureLevelValidatorTest {

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
    void testRaiseLevelOnceAllTabletServersSupportIt() throws Exception {
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        registerTabletServer(1, ClusterFeatures.supportedFeatures());
        ClusterFeatureLevelValidator validator = newValidator();

        assertThatCode(() -> validator.validate(withLevel(3))).doesNotThrowAnyException();
    }

    @Test
    void testRaiseLevelRejectedWhileATabletServerDoesNotSupportIt() throws Exception {
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        // registered before cluster features existed
        registerTabletServer(1, Collections.emptyMap());
        ClusterFeatureLevelValidator validator = newValidator();

        assertThatThrownBy(() -> validator.validate(withLevel(3)))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("tablet servers [1] do not support it");
    }

    @Test
    void testLowerLevelRejected() throws Exception {
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        ClusterFeatureLevelValidator validator = newValidator();
        validator.reconfigure(withLevel(3));

        assertThatThrownBy(() -> validator.validate(withLevel(2)))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Cannot lower cluster feature");
    }

    @Test
    void testUnsupportedLevelRejected() {
        ClusterFeatureLevelValidator validator = newValidator();

        assertThatThrownBy(() -> validator.validate(withLevel(4)))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("supported levels are 2 to 3");
        assertThatThrownBy(() -> validator.validate(withLevel(1)))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("supported levels are 2 to 3");
    }

    private static ClusterFeatureLevelValidator newValidator() {
        return new ClusterFeatureLevelValidator(zkClient, new Configuration());
    }

    private static Configuration withLevel(int level) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLUSTER_FEATURE_LOG_RECORD_BATCH_VERSION, level);
        return conf;
    }

    private static void registerTabletServer(int serverId, Map<String, Integer> supportedFeatures)
            throws Exception {
        zkClient.registerTabletServer(
                serverId,
                new TabletServerRegistration(
                        null,
                        Endpoint.fromListenersString("CLIENT://localhost:" + (9120 + serverId)),
                        System.currentTimeMillis(),
                        TabletServerResource.unknown(),
                        supportedFeatures));
    }
}
