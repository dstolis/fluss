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

package org.apache.fluss.client.admin;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.exception.InvalidClusterFeatureUpdateException;
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData.ServerIdZNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ITCase for describing and raising cluster features on a cluster that was upgraded from a version
 * without cluster features, so it starts with every feature at its baseline level.
 */
class ClusterFeaturesITCase {

    private static final String FEATURE = ClusterFeature.LOG_RECORD_BATCH_VERSION.featureName();
    private static final int OLD_TABLET_SERVER_ID = 99;

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setBaselineClusterFeatureLevels(true)
                    .build();

    private Connection conn;
    private Admin admin;

    @BeforeEach
    void setup() {
        conn = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        admin = conn.getAdmin();
    }

    @AfterEach
    void teardown() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    void testDescribeValidateAndRaise() throws Exception {
        // an upgraded cluster is at the baseline, and every server supports the latest level
        assertThat(describe()).isEqualTo(new ClusterFeatureDescription(FEATURE, 2, 3));
        assertAppliedLevelOnTabletServers(2);

        // validating a raise changes nothing
        admin.updateClusterFeatures(level(3), true).get();
        assertThat(describe().finalizedLevel()).isEqualTo(2);

        // invalid changes are rejected and change nothing
        assertThatThrownBy(() -> admin.updateClusterFeatures(level(4), false).get())
                .hasRootCauseInstanceOf(InvalidClusterFeatureUpdateException.class)
                .hasMessageContaining("supports levels 2 to 3");
        assertThatThrownBy(
                        () ->
                                admin.updateClusterFeatures(
                                                Collections.singletonMap("no.such-feature", 1),
                                                false)
                                        .get())
                .hasRootCauseInstanceOf(InvalidClusterFeatureUpdateException.class)
                .hasMessageContaining("Unknown cluster feature");
        assertThat(describe().finalizedLevel()).isEqualTo(2);

        // the raise is saved and every tablet server follows it
        admin.updateClusterFeatures(level(3), false).get();
        assertThat(describe()).isEqualTo(new ClusterFeatureDescription(FEATURE, 3, 3));
        assertAppliedLevelOnTabletServers(3);

        // a tablet server of a version without cluster features that registers now is kept out,
        // and like a broker whose registration Kafka refused, it does not lower the supported level
        registerTabletServerWithoutFeatures(OLD_TABLET_SERVER_ID);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        FLUSS_CLUSTER_EXTENSION
                                                .getCoordinatorServer()
                                                .getCoordinatorEventProcessor()
                                                .getCoordinatorContext()
                                                .liveTabletServerSet())
                                .containsExactlyInAnyOrder(0, 1, 2));
        assertThat(describe()).isEqualTo(new ClusterFeatureDescription(FEATURE, 3, 3));
        FLUSS_CLUSTER_EXTENSION
                .getZooKeeperClient()
                .getCuratorClient()
                .delete()
                .forPath(ServerIdZNode.path(OLD_TABLET_SERVER_ID));

        // a level is never lowered
        assertThatThrownBy(() -> admin.updateClusterFeatures(level(2), false).get())
                .hasRootCauseInstanceOf(InvalidClusterFeatureUpdateException.class)
                .hasMessageContaining("Cannot lower cluster feature");
        assertThat(describe().finalizedLevel()).isEqualTo(3);

        // the level survives a restart of the coordinator and of a tablet server
        FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
        FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        retry(Duration.ofMinutes(1), () -> assertThat(describe().finalizedLevel()).isEqualTo(3));
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(0);
        FLUSS_CLUSTER_EXTENSION.startTabletServer(0);
        assertAppliedLevelOnTabletServers(3);
    }

    private static void registerTabletServerWithoutFeatures(int serverId) throws Exception {
        FLUSS_CLUSTER_EXTENSION
                .getZooKeeperClient()
                .registerTabletServer(
                        serverId,
                        new TabletServerRegistration(
                                null,
                                Endpoint.fromListenersString("FLUSS://localhost:9999"),
                                System.currentTimeMillis(),
                                TabletServerResource.unknown(),
                                Collections.emptyMap()));
    }

    private ClusterFeatureDescription describe() throws Exception {
        Collection<ClusterFeatureDescription> features = admin.describeClusterFeatures().get();
        assertThat(features).hasSize(ClusterFeature.values().length);
        return features.stream().filter(f -> f.name().equals(FEATURE)).findFirst().get();
    }

    private static Map<String, Integer> level(int level) {
        return Collections.singletonMap(FEATURE, level);
    }

    private static void assertAppliedLevelOnTabletServers(int level) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    for (TabletServer server : FLUSS_CLUSTER_EXTENSION.getTabletServers()) {
                        assertThat(
                                        server.getReplicaManager()
                                                .getClusterFeatures()
                                                .levels()
                                                .level(ClusterFeature.LOG_RECORD_BATCH_VERSION))
                                .isEqualTo(level);
                    }
                });
    }
}
