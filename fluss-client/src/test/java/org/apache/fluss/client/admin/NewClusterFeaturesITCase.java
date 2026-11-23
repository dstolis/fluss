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
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for the cluster features of a new cluster, which start at their latest levels. */
class NewClusterFeaturesITCase {

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    @Test
    void testNewClusterStartsAtTheLatestLevels() throws Exception {
        try (Connection conn =
                        ConnectionFactory.createConnection(
                                FLUSS_CLUSTER_EXTENSION.getClientConfig());
                Admin admin = conn.getAdmin()) {
            assertThat(admin.describeClusterFeatures().get().features())
                    .allSatisfy(
                            feature -> {
                                ClusterFeature known = ClusterFeature.forName(feature.name());
                                assertThat(known).isNotNull();
                                assertThat(feature.finalizedLevel()).isEqualTo(known.latestLevel());
                                assertThat(feature.supportedLevel()).isEqualTo(known.latestLevel());
                            })
                    .hasSize(ClusterFeature.values().length);
        }
        // the tablet servers follow the levels without anyone raising them
        retry(
                Duration.ofMinutes(1),
                () -> {
                    for (TabletServer server : FLUSS_CLUSTER_EXTENSION.getTabletServers()) {
                        for (ClusterFeature feature : ClusterFeature.values()) {
                            assertThat(
                                            server.getReplicaManager()
                                                    .getClusterFeatures()
                                                    .levels()
                                                    .level(feature))
                                    .isEqualTo(feature.latestLevel());
                        }
                    }
                });
    }
}
