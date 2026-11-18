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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.server.ServerBase;
import org.apache.fluss.server.ServerTestBase;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.ZkVersion;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CoordinatorServer} . */
class CoordinatorServerTest extends ServerTestBase {

    private CoordinatorServer coordinatorServer;

    @BeforeEach
    void beforeEach() throws Exception {
        coordinatorServer = startCoordinatorServer(createConfiguration());
        waitUntilCoordinatorServerElected();
    }

    @AfterEach
    void after() throws Exception {
        if (coordinatorServer != null) {
            coordinatorServer.close();
        }
    }

    @Override
    protected ServerBase getServer() {
        return coordinatorServer;
    }

    @Test
    void testRefusesToStartBelowAFinalizedClusterFeatureLevel() throws Exception {
        coordinatorServer.close();
        coordinatorServer = null;
        // a later version finalized a cluster feature that this version does not know, and a
        // coordinator that cannot read data of that level must not lead the cluster
        zookeeperClient.updateClusterFeatureLevels(
                Collections.singletonMap("added-by-a-later-version", 1),
                zookeeperClient.getClusterFeatureLevels().get().zkVersion(),
                ZkVersion.MATCH_ANY_VERSION.getVersion());
        CoordinatorServer server = new CoordinatorServer(createConfiguration());
        try {
            assertThatThrownBy(server::start)
                    .rootCause()
                    .isInstanceOf(IllegalConfigurationException.class)
                    .hasMessageContaining("does not support the finalized cluster features")
                    .hasMessageContaining("added-by-a-later-version");
        } finally {
            server.close();
            zookeeperClient.getCuratorClient().delete().forPath(ZkData.ClusterFeaturesZNode.path());
        }
    }

    @Override
    protected ServerBase getStartFailServer() {
        Configuration configuration = createConfiguration();
        // CoordinatorServer starts leader services asynchronously in a separate election
        // thread. An invalid port wouldn't cause start() to throw because the port binding
        // happens asynchronously. Instead, use an empty ZK address to cause a synchronous
        // failure in startZookeeperClient() during start().
        configuration.setString(ConfigOptions.ZOOKEEPER_ADDRESS, "");
        return new CoordinatorServer(configuration);
    }

    @Override
    protected void checkAfterStartServer() throws Exception {
        assertThat(coordinatorServer.getRpcServer()).isNotNull();
        // check the data put in zk after coordinator server start
        Optional<CoordinatorAddress> optCoordinatorAddr =
                zookeeperClient.getCoordinatorLeaderAddress();
        assertThat(optCoordinatorAddr).isNotEmpty();
        verifyEndpoint(
                optCoordinatorAddr.get().getEndpoints(),
                coordinatorServer.getRpcServer().getBindEndpoints());
    }

    public void waitUntilCoordinatorServerElected() {
        waitUntil(
                () -> zookeeperClient.getCoordinatorLeaderAddress().isPresent(),
                Duration.ofSeconds(5),
                "Fail to wait coordinator server elected");
    }
}
