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

import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ClusterFeaturesWatcher}. */
class ClusterFeaturesWatcherTest {

    private static final int ANY_COORDINATOR = ZkVersion.MATCH_ANY_VERSION.getVersion();

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
    void testFollowsTheStoredLevelsAndNeverGoesBack() throws Exception {
        FinalizedClusterFeatures features = new FinalizedClusterFeatures();
        CompletableFuture<Throwable> fatalError = new CompletableFuture<>();
        try (ClusterFeaturesWatcher watcher =
                new ClusterFeaturesWatcher(zkClient, features, fatalError::complete)) {
            // the levels do not exist yet, like on a tablet server that starts before the first
            // coordinator of a new cluster
            watcher.start();
            assertThat(features.levels()).isEqualTo(ClusterFeatureLevels.baseline());

            zkClient.createClusterFeatureLevels(
                    ClusterFeatureLevels.baseline().toMap(), ANY_COORDINATOR);
            int version = zkClient.getClusterFeatureLevels().get().zkVersion();
            zkClient.updateClusterFeatureLevels(
                    ClusterFeatureLevels.latest().toMap(), version, ANY_COORDINATOR);
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(features.levels()).isEqualTo(ClusterFeatureLevels.latest()));

            // a lower stored level must never take effect, whoever wrote it
            zkClient.updateClusterFeatureLevels(
                    ClusterFeatureLevels.baseline().toMap(), version + 1, ANY_COORDINATOR);
            // a level this server does not support is a fatal error
            zkClient.updateClusterFeatureLevels(
                    Collections.singletonMap(LOG_RECORD_BATCH_VERSION.featureName(), 4),
                    version + 2,
                    ANY_COORDINATOR);
            assertThat(fatalError.get(1, TimeUnit.MINUTES))
                    .hasMessageContaining("log.record-batch.version (supports 3, finalized 4)");
            assertThat(features.levels()).isEqualTo(ClusterFeatureLevels.latest());
        }
    }
}
