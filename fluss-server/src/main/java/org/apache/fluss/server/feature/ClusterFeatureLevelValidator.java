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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.TabletServerRegistration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.server.feature.ClusterFeatures.LOG_RECORD_BATCH_VERSION;
import static org.apache.fluss.server.feature.ClusterFeatures.LOG_RECORD_BATCH_VERSION_BASELINE;
import static org.apache.fluss.server.feature.ClusterFeatures.LOG_RECORD_BATCH_VERSION_MAX;

/**
 * Validates changes to the finalized level of {@link ClusterFeatures#LOG_RECORD_BATCH_VERSION} on
 * the coordinator before they are persisted. A level can only be raised once every registered
 * tablet server supports it, and can never be lowered.
 */
@Internal
public class ClusterFeatureLevelValidator implements ServerReconfigurable {

    private final ZooKeeperClient zkClient;
    private volatile int finalizedLevel;

    public ClusterFeatureLevelValidator(ZooKeeperClient zkClient, Configuration conf) {
        this.zkClient = zkClient;
        this.finalizedLevel = conf.get(ConfigOptions.CLUSTER_FEATURE_LOG_RECORD_BATCH_VERSION);
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        int newLevel = newConfig.get(ConfigOptions.CLUSTER_FEATURE_LOG_RECORD_BATCH_VERSION);
        if (newLevel == finalizedLevel) {
            return;
        }
        if (newLevel < LOG_RECORD_BATCH_VERSION_BASELINE
                || newLevel > LOG_RECORD_BATCH_VERSION_MAX) {
            throw new ConfigException(
                    String.format(
                            "Invalid level %s for cluster feature '%s', supported levels are %s to %s.",
                            newLevel,
                            LOG_RECORD_BATCH_VERSION,
                            LOG_RECORD_BATCH_VERSION_BASELINE,
                            LOG_RECORD_BATCH_VERSION_MAX));
        }
        if (newLevel < finalizedLevel) {
            throw new ConfigException(
                    String.format(
                            "Cannot lower cluster feature '%s' from %s to %s, because data written "
                                    + "at level %s may not be readable at a lower level.",
                            LOG_RECORD_BATCH_VERSION, finalizedLevel, newLevel, finalizedLevel));
        }
        List<Integer> unsupportedServers = tabletServersNotSupporting(newLevel);
        if (!unsupportedServers.isEmpty()) {
            throw new ConfigException(
                    String.format(
                            "Cannot raise cluster feature '%s' to %s, because tablet servers %s do "
                                    + "not support it. Upgrade them first.",
                            LOG_RECORD_BATCH_VERSION, newLevel, unsupportedServers));
        }
    }

    @Override
    public void reconfigure(Configuration newConfig) {
        finalizedLevel = newConfig.get(ConfigOptions.CLUSTER_FEATURE_LOG_RECORD_BATCH_VERSION);
    }

    private List<Integer> tabletServersNotSupporting(int level) throws ConfigException {
        Map<Integer, TabletServerRegistration> registrations;
        try {
            registrations = zkClient.getTabletServers(zkClient.getSortedTabletServerList());
        } catch (Exception e) {
            throw new ConfigException(
                    "Failed to read the tablet server registrations: " + e.getMessage());
        }
        List<Integer> unsupported = new ArrayList<>();
        registrations.forEach(
                (serverId, registration) -> {
                    int supported =
                            ClusterFeatures.supportedLevel(
                                    registration.getSupportedFeatures(), LOG_RECORD_BATCH_VERSION);
                    if (supported < level) {
                        unsupported.add(serverId);
                    }
                });
        return unsupported;
    }
}
