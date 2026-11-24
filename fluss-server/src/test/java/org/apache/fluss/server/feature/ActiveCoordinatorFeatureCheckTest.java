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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.exception.InvalidCoordinatorException;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.AppliedClusterFeatures;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ActiveCoordinatorFeatureCheck} and the applied levels tablet servers report. */
class ActiveCoordinatorFeatureCheckTest {

    private static final List<Endpoint> ENDPOINTS =
            Collections.singletonList(new Endpoint("localhost", 9123, "FLUSS"));

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
    void testCoordinatorWithoutClusterFeaturesIsRefusedOnceALevelIsRaised() throws Exception {
        // a coordinator of a version without cluster features registers no supported levels
        int epoch = becomeActive(new CoordinatorAddress("old", ENDPOINTS));
        FinalizedClusterFeatures features = new FinalizedClusterFeatures();
        ActiveCoordinatorFeatureCheck check = new ActiveCoordinatorFeatureCheck(zkClient, features);

        // it supports the baselines, so it may lead a cluster at the baselines
        check.check(epoch, "notifyLeaderAndIsr");

        features.apply(ClusterFeatureLevels.latest(), 1);
        assertThatThrownBy(() -> check.check(epoch, "notifyLeaderAndIsr"))
                .isInstanceOf(InvalidCoordinatorException.class)
                .hasMessageContaining("the active coordinator old of coordinator epoch " + epoch)
                .hasMessageContaining("log.record-batch.version (supports [2, 2], finalized 3)");
        assertThatThrownBy(() -> check.check(epoch, "updateMetadataCache"))
                .isInstanceOf(InvalidCoordinatorException.class);
    }

    @Test
    void testCoordinatorThatSupportsTheLevelsIsFollowed() throws Exception {
        FinalizedClusterFeatures features = new FinalizedClusterFeatures();
        features.apply(ClusterFeatureLevels.latest(), 1);
        ActiveCoordinatorFeatureCheck check = new ActiveCoordinatorFeatureCheck(zkClient, features);
        int oldEpoch = becomeActive(new CoordinatorAddress("old", ENDPOINTS));
        assertThatThrownBy(() -> check.check(oldEpoch, "notifyLeaderAndIsr"))
                .isInstanceOf(InvalidCoordinatorException.class);

        // a coordinator that supports the levels takes over
        zkClient.unregisterCoordinatorLeader(new CoordinatorAddress("old", ENDPOINTS));
        int newEpoch =
                becomeActive(
                        new CoordinatorAddress(
                                "new", ENDPOINTS, ClusterFeatures.supportedFeatures()));
        check.check(newEpoch, "notifyLeaderAndIsr");
    }

    @Test
    void testRequestOfAnotherEpochIsLeftToTheEpochFencing() throws Exception {
        FinalizedClusterFeatures features = new FinalizedClusterFeatures();
        features.apply(ClusterFeatureLevels.latest(), 1);
        int epoch = becomeActive(new CoordinatorAddress("old", ENDPOINTS));
        ActiveCoordinatorFeatureCheck check = new ActiveCoordinatorFeatureCheck(zkClient, features);

        // the active coordinator in ZooKeeper is not the one that sent the request
        check.check(epoch + 1, "notifyLeaderAndIsr");
    }

    @Test
    void testTabletServersReportTheirAppliedLevels() throws Exception {
        Map<String, Integer> baseline = Collections.singletonMap("log.record-batch.version", 2);
        Map<String, Integer> latest = Collections.singletonMap("log.record-batch.version", 3);
        zkClient.reportAppliedClusterFeatures(1, new AppliedClusterFeatures(0, baseline));
        zkClient.reportAppliedClusterFeatures(2, new AppliedClusterFeatures(0, baseline));
        zkClient.reportAppliedClusterFeatures(1, new AppliedClusterFeatures(1, latest));

        // server 3 has not reported, like a server without cluster features
        Map<Integer, AppliedClusterFeatures> applied =
                zkClient.getAppliedClusterFeatures(new int[] {1, 2, 3});
        assertThat(applied)
                .containsOnlyKeys(1, 2)
                .containsEntry(1, new AppliedClusterFeatures(1, latest))
                .containsEntry(2, new AppliedClusterFeatures(0, baseline));
        assertThat(AppliedClusterFeatures.fromJsonBytes(applied.get(1).toJsonBytes()))
                .isEqualTo(applied.get(1));
    }

    private static int becomeActive(CoordinatorAddress address) throws Exception {
        zkClient.ensureEpochZnodeExists();
        ZkEpoch epoch = zkClient.fenceBecomeCoordinatorLeader(address.getId());
        zkClient.registerCoordinatorLeader(address);
        return epoch.getCoordinatorEpoch();
    }
}
