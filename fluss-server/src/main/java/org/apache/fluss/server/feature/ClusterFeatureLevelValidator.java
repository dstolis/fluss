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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.TabletServerRegistration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Validates changes of the finalized level of every {@link ClusterFeature} on the coordinator. A
 * level can only be raised once every registered tablet server supports it, and it can never be
 * lowered, because data written at a higher level may not be readable at a lower one.
 */
@Internal
public class ClusterFeatureLevelValidator implements ServerReconfigurable {

    private final ZooKeeperClient zkClient;
    private volatile ClusterFeatureLevels finalized;

    public ClusterFeatureLevelValidator(ZooKeeperClient zkClient, Configuration conf) {
        this.zkClient = zkClient;
        this.finalized = ClusterFeatureLevels.fromConfig(conf);
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        ClusterFeatureLevels requested = ClusterFeatureLevels.fromConfig(newConfig);
        Map<Integer, TabletServerRegistration> registrations = null;
        for (ClusterFeature feature : ClusterFeature.values()) {
            int current = finalized.level(feature);
            int level = requested.level(feature);
            if (level == current) {
                continue;
            }
            if (level < feature.baselineLevel() || level > feature.latestLevel()) {
                throw new ConfigException(
                        String.format(
                                "Invalid level %s for cluster feature '%s', supported levels are %s to %s.",
                                level,
                                feature.featureName(),
                                feature.baselineLevel(),
                                feature.latestLevel()));
            }
            if (level < current) {
                throw new ConfigException(
                        String.format(
                                "Cannot lower cluster feature '%s' from %s to %s, because data "
                                        + "written at level %s may not be readable at a lower level.",
                                feature.featureName(), current, level, current));
            }
            if (registrations == null) {
                registrations = tabletServers();
            }
            List<Integer> unsupported = new ArrayList<>();
            registrations.forEach(
                    (serverId, registration) -> {
                        if (ClusterFeatures.supportedLevel(
                                        registration.getSupportedFeatures(), feature)
                                < level) {
                            unsupported.add(serverId);
                        }
                    });
            if (!unsupported.isEmpty()) {
                throw new ConfigException(
                        String.format(
                                "Cannot raise cluster feature '%s' to %s, because tablet servers "
                                        + "%s do not support it. Upgrade them first.",
                                feature.featureName(), level, unsupported));
            }
        }
    }

    @Override
    public void reconfigure(Configuration newConfig) {
        finalized = ClusterFeatureLevels.fromConfig(newConfig);
    }

    private Map<Integer, TabletServerRegistration> tabletServers() throws ConfigException {
        try {
            return zkClient.getTabletServers(zkClient.getSortedTabletServerList());
        } catch (Exception e) {
            throw new ConfigException(
                    "Failed to read the tablet server registrations: " + e.getMessage());
        }
    }
}
