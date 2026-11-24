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
import org.apache.fluss.exception.InvalidClusterFeatureUpdateException;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.rpc.protocol.feature.SupportedLevelRange;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ClusterFeatureManager}. */
class ClusterFeatureManagerTest {

    private static final String FEATURE = LOG_RECORD_BATCH_VERSION.featureName();
    private static final Map<String, SupportedLevelRange> NO_FEATURES = Collections.emptyMap();

    @RegisterExtension
    static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zkClient;

    /** The tablet servers the coordinator admitted, which {@link #registerTabletServer} adds to. */
    private final Set<Integer> admitted = new HashSet<>();

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
    void testNewClusterStartsAtTheLatestLevels() throws Exception {
        int epochZkVersion = becomeLeader();

        assertThat(new ClusterFeatureManager(zkClient).initialize(epochZkVersion))
                .isEqualTo(ClusterFeatureLevels.latest());
        assertThat(zkClient.getClusterFeatureLevels().get().data())
                .containsOnly(Collections.singletonMap(FEATURE, 3).entrySet().iterator().next());
    }

    @Test
    void testUpgradedClusterStartsAtTheBaselines() throws Exception {
        // a cluster that ran a version without cluster features has databases but no levels
        zkClient.registerDatabase(
                "fluss", DatabaseRegistration.of(DatabaseDescriptor.builder().build()));
        int epochZkVersion = becomeLeader();

        assertThat(new ClusterFeatureManager(zkClient).initialize(epochZkVersion))
                .isEqualTo(ClusterFeatureLevels.baseline());
        // the levels are stored, so the cluster is not taken for a new one later
        assertThat(zkClient.getClusterFeatureLevels().get().data()).isEmpty();
    }

    @Test
    void testCoordinatorRefusesToStartAtLevelsItDoesNotSupport() {
        // a later version that supports only level 3 can't start an upgraded cluster at baseline 2
        Map<String, SupportedLevelRange> onlyLevel3 =
                Collections.singletonMap(FEATURE, new SupportedLevelRange(3, 3));
        assertThatThrownBy(
                        () ->
                                ClusterFeatureManager.checkCanStartAt(
                                        ClusterFeatureLevels.baseline(), onlyLevel3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not support")
                .hasMessageContaining("upgrade it to a version that supports them first");
        ClusterFeatureManager.checkCanStartAt(ClusterFeatureLevels.latest(), onlyLevel3);
        ClusterFeatureManager.checkCanStartAt(
                ClusterFeatureLevels.baseline(), ClusterFeatures.supportedFeatures());
    }

    @Test
    void testStoredLevelsAreKeptWhenACoordinatorTakesOver() throws Exception {
        zkClient.registerDatabase(
                "fluss", DatabaseRegistration.of(DatabaseDescriptor.builder().build()));
        ClusterFeatureManager manager = new ClusterFeatureManager(zkClient);
        assertThat(manager.initialize(becomeLeader())).isEqualTo(ClusterFeatureLevels.baseline());

        // also when every database was dropped in the meantime
        zkClient.deleteDatabase("fluss");
        assertThat(manager.initialize(becomeLeader())).isEqualTo(ClusterFeatureLevels.baseline());
    }

    @Test
    void testRaiseLevelOnceEveryServerSupportsIt() throws Exception {
        int epochZkVersion = startUpgradedCluster();
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        registerTabletServer(1, ClusterFeatures.supportedFeatures());
        registerCoordinator("c0", ClusterFeatures.supportedFeatures());
        ClusterFeatureManager manager = new ClusterFeatureManager(zkClient);
        assertThat(manager.supportedLevels(admitted).level(LOG_RECORD_BATCH_VERSION)).isEqualTo(3);

        assertThat(
                        manager.update(level(3), false, null, epochZkVersion, admitted)
                                .levels()
                                .level(LOG_RECORD_BATCH_VERSION))
                .isEqualTo(3);
        assertThat(manager.finalizedLevels().level(LOG_RECORD_BATCH_VERSION)).isEqualTo(3);
        // raising to the finalized level again changes nothing
        assertThat(
                        manager.update(level(3), false, null, epochZkVersion, admitted)
                                .levels()
                                .level(LOG_RECORD_BATCH_VERSION))
                .isEqualTo(3);
    }

    @Test
    void testValidateOnlyChangesNothing() throws Exception {
        int epochZkVersion = startUpgradedCluster();
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        ClusterFeatureManager manager = new ClusterFeatureManager(zkClient);

        assertThat(
                        manager.update(level(3), true, null, epochZkVersion, admitted)
                                .levels()
                                .level(LOG_RECORD_BATCH_VERSION))
                .isEqualTo(3);
        assertThat(manager.finalizedLevels()).isEqualTo(ClusterFeatureLevels.baseline());

        // and it reports the same rejection as the raise itself
        registerTabletServer(1, NO_FEATURES);
        assertThatThrownBy(() -> manager.update(level(3), true, null, epochZkVersion, admitted))
                .isInstanceOf(InvalidClusterFeatureUpdateException.class)
                .hasMessageContaining("[tablet server 1] do not support it");
    }

    @Test
    void testRaiseLevelRejectedWhileATabletServerDoesNotSupportIt() throws Exception {
        int epochZkVersion = startUpgradedCluster();
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        // a tablet server of a version without cluster features registers none
        registerTabletServer(1, NO_FEATURES);
        ClusterFeatureManager manager = new ClusterFeatureManager(zkClient);
        assertThat(manager.supportedLevels(admitted).level(LOG_RECORD_BATCH_VERSION)).isEqualTo(2);

        assertThatThrownBy(() -> manager.update(level(3), false, null, epochZkVersion, admitted))
                .isInstanceOf(InvalidClusterFeatureUpdateException.class)
                .hasMessageContaining("[tablet server 1] do not support it");
        assertThat(manager.finalizedLevels()).isEqualTo(ClusterFeatureLevels.baseline());
    }

    @Test
    void testRaiseLevelRejectedWhileACoordinatorDoesNotSupportIt() throws Exception {
        int epochZkVersion = startUpgradedCluster();
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        registerCoordinator("c0", ClusterFeatures.supportedFeatures());
        // a standby coordinator of a version without cluster features
        registerCoordinator("c1", NO_FEATURES);
        ClusterFeatureManager manager = new ClusterFeatureManager(zkClient);

        assertThatThrownBy(() -> manager.update(level(3), false, null, epochZkVersion, admitted))
                .isInstanceOf(InvalidClusterFeatureUpdateException.class)
                .hasMessageContaining("[coordinator c1] do not support it");
        assertThat(manager.finalizedLevels()).isEqualTo(ClusterFeatureLevels.baseline());
    }

    @Test
    void testRaiseLevelRejectedWhileATabletServerWithReplicasIsOffline() throws Exception {
        int epochZkVersion = startUpgradedCluster();
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        // server 5 holds a replica but is not registered, for example while it restarts
        zkClient.registerTableAssignment(
                1L, TableAssignment.builder().add(0, BucketAssignment.of(0, 5)).build());
        ClusterFeatureManager manager = new ClusterFeatureManager(zkClient);

        assertThatThrownBy(() -> manager.update(level(3), false, null, epochZkVersion, admitted))
                .isInstanceOf(InvalidClusterFeatureUpdateException.class)
                .hasMessageContaining("tablet servers [5] hold replicas but are offline");

        registerTabletServer(5, ClusterFeatures.supportedFeatures());
        assertThat(
                        manager.update(level(3), false, null, epochZkVersion, admitted)
                                .levels()
                                .level(LOG_RECORD_BATCH_VERSION))
                .isEqualTo(3);
    }

    @Test
    void testRaiseRejectedWhileAnExitedServerIsStillAdmitted() throws Exception {
        int epochZkVersion = startUpgradedCluster();
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        // server 5 holds a replica and has just exited: its registration is gone, but the
        // coordinator has not processed that yet, so it still counts as admitted
        admitted.add(5);
        zkClient.registerTableAssignment(
                1L, TableAssignment.builder().add(0, BucketAssignment.of(0, 5)).build());
        ClusterFeatureManager manager = new ClusterFeatureManager(zkClient);

        assertThatThrownBy(() -> manager.update(level(3), false, null, epochZkVersion, admitted))
                .isInstanceOf(InvalidClusterFeatureUpdateException.class)
                .hasMessageContaining("tablet servers [5] hold replicas but are offline");
    }

    @Test
    void testTabletServerKeptOutDoesNotBlockARaise() throws Exception {
        int epochZkVersion = startUpgradedCluster();
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        // server 1 runs a version without cluster features and holds a replica, but the
        // coordinator kept it out, as it does when such a server comes back after a raise
        registerKeptOutTabletServer(1, NO_FEATURES);
        zkClient.registerTableAssignment(
                1L, TableAssignment.builder().add(0, BucketAssignment.of(0, 1)).build());
        ClusterFeatureManager manager = new ClusterFeatureManager(zkClient);

        // it serves no data, so like a broker whose registration Kafka refused, it does not count
        assertThat(manager.supportedLevels(admitted).level(LOG_RECORD_BATCH_VERSION)).isEqualTo(3);
        assertThat(
                        manager.update(level(3), false, null, epochZkVersion, admitted)
                                .levels()
                                .level(LOG_RECORD_BATCH_VERSION))
                .isEqualTo(3);

        // once admitted, it counts
        admitted.add(1);
        assertThat(manager.supportedLevels(admitted).level(LOG_RECORD_BATCH_VERSION)).isEqualTo(2);
    }

    @Test
    void testFeatureEpochGoesUpWithEveryRaise() throws Exception {
        int epochZkVersion = startUpgradedCluster();
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        ClusterFeatureManager manager = new ClusterFeatureManager(zkClient);
        assertThat(manager.finalizedState().epoch()).isEqualTo(0);

        // validating does not change the epoch
        assertThat(manager.update(level(3), true, 0, epochZkVersion, admitted).epoch())
                .isEqualTo(0);
        assertThat(manager.update(level(3), false, 0, epochZkVersion, admitted).epoch())
                .isEqualTo(1);
        assertThat(manager.finalizedState().epoch()).isEqualTo(1);
        // a raise to the levels the cluster already has changes nothing
        assertThat(manager.update(level(3), false, null, epochZkVersion, admitted).epoch())
                .isEqualTo(1);
    }

    @Test
    void testRaiseRejectedWhenTheFeatureEpochChanged() throws Exception {
        int epochZkVersion = startUpgradedCluster();
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        ClusterFeatureManager manager = new ClusterFeatureManager(zkClient);
        int describedEpoch = manager.finalizedState().epoch();
        // the stored levels are written again after the operator described them
        zkClient.updateClusterFeatureLevels(Collections.emptyMap(), describedEpoch, epochZkVersion);

        assertThatThrownBy(
                        () ->
                                manager.update(
                                        level(3), true, describedEpoch, epochZkVersion, admitted))
                .isInstanceOf(InvalidClusterFeatureUpdateException.class)
                .hasMessageContaining("the feature epoch is 1, not the expected 0");
        assertThatThrownBy(
                        () ->
                                manager.update(
                                        level(3), false, describedEpoch, epochZkVersion, admitted))
                .isInstanceOf(InvalidClusterFeatureUpdateException.class);
        assertThat(manager.finalizedLevels()).isEqualTo(ClusterFeatureLevels.baseline());

        // after describing again, the raise goes through
        int currentEpoch = manager.finalizedState().epoch();
        assertThat(manager.update(level(3), false, currentEpoch, epochZkVersion, admitted).epoch())
                .isEqualTo(currentEpoch + 1);
        // and a retry with the old epoch succeeds, because the cluster already has those levels
        assertThat(manager.update(level(3), false, currentEpoch, epochZkVersion, admitted).epoch())
                .isEqualTo(currentEpoch + 1);
    }

    @Test
    void testInvalidUpdatesAreRejected() throws Exception {
        int epochZkVersion = startUpgradedCluster();
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        ClusterFeatureManager manager = new ClusterFeatureManager(zkClient);

        assertThatThrownBy(() -> manager.update(level(4), false, null, epochZkVersion, admitted))
                .isInstanceOf(InvalidClusterFeatureUpdateException.class)
                .hasMessageContaining("supports levels [2, 3]");
        assertThatThrownBy(
                        () ->
                                manager.update(
                                        Collections.singletonMap("no.such-feature", 1),
                                        false,
                                        null,
                                        epochZkVersion,
                                        admitted))
                .isInstanceOf(InvalidClusterFeatureUpdateException.class)
                .hasMessageContaining("Unknown cluster feature 'no.such-feature'");

        // a level is never lowered
        manager.update(level(3), false, null, epochZkVersion, admitted);
        assertThatThrownBy(() -> manager.update(level(2), false, null, epochZkVersion, admitted))
                .isInstanceOf(InvalidClusterFeatureUpdateException.class)
                .hasMessageContaining("Cannot lower cluster feature");
        assertThat(manager.finalizedLevels().level(LOG_RECORD_BATCH_VERSION)).isEqualTo(3);
    }

    @Test
    void testCoordinatorThatLostLeadershipCannotChangeTheLevels() throws Exception {
        int staleEpochZkVersion = startUpgradedCluster();
        registerTabletServer(0, ClusterFeatures.supportedFeatures());
        ClusterFeatureManager manager = new ClusterFeatureManager(zkClient);
        // another coordinator takes leadership
        becomeLeader();

        assertThatThrownBy(
                        () -> manager.update(level(3), false, null, staleEpochZkVersion, admitted))
                .isInstanceOf(NotCoordinatorLeaderException.class);
        assertThat(manager.finalizedLevels()).isEqualTo(ClusterFeatureLevels.baseline());
    }

    @Test
    void testWriteBasedOnAnOlderReadFails() throws Exception {
        int epochZkVersion = startUpgradedCluster();
        int readVersion = zkClient.getClusterFeatureLevels().get().zkVersion();
        zkClient.updateClusterFeatureLevels(level(3), readVersion, epochZkVersion);

        // a writer that still holds the older read must not put the lower levels back
        assertThatThrownBy(
                        () ->
                                zkClient.updateClusterFeatureLevels(
                                        Collections.emptyMap(), readVersion, epochZkVersion))
                .isInstanceOf(KeeperException.BadVersionException.class);
        assertThat(zkClient.getClusterFeatureLevels().get().data()).containsEntry(FEATURE, 3);
    }

    /** Stores the baseline levels like an upgraded cluster and returns the epoch zk version. */
    private static int startUpgradedCluster() throws Exception {
        zkClient.registerDatabase(
                "fluss", DatabaseRegistration.of(DatabaseDescriptor.builder().build()));
        int epochZkVersion = becomeLeader();
        new ClusterFeatureManager(zkClient).initialize(epochZkVersion);
        return epochZkVersion;
    }

    /** Takes coordinator leadership and returns the zk version of the coordinator epoch. */
    private static int becomeLeader() throws Exception {
        return zkClient.fenceBecomeCoordinatorLeader("c0").getCoordinatorEpochZkVersion();
    }

    private static Map<String, Integer> level(int level) {
        return Collections.singletonMap(FEATURE, level);
    }

    private static void registerCoordinator(
            String id, Map<String, SupportedLevelRange> supportedFeatures) throws Exception {
        zkClient.registerCoordinatorServer(
                new CoordinatorAddress(
                        id,
                        Endpoint.fromListenersString("CLIENT://localhost:9123"),
                        supportedFeatures));
    }

    /** Registers a tablet server that the coordinator admitted. */
    private void registerTabletServer(
            int serverId, Map<String, SupportedLevelRange> supportedFeatures) throws Exception {
        registerKeptOutTabletServer(serverId, supportedFeatures);
        admitted.add(serverId);
    }

    /** Registers a tablet server that the coordinator did not admit. */
    private static void registerKeptOutTabletServer(
            int serverId, Map<String, SupportedLevelRange> supportedFeatures) throws Exception {
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
