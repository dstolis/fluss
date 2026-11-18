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
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** The cluster features a server registers and checks, see {@link ClusterFeature}. */
@Internal
public final class ClusterFeatures {

    private ClusterFeatures() {}

    /** The features this server supports, mapped to the highest supported level. */
    public static Map<String, Integer> supportedFeatures() {
        Map<String, Integer> supported = new HashMap<>();
        for (ClusterFeature feature : ClusterFeature.values()) {
            supported.put(feature.featureName(), feature.latestLevel());
        }
        return Collections.unmodifiableMap(supported);
    }

    /**
     * Returns the highest level of the feature in the supported features a server registered.
     * Servers that registered before the feature existed support its baseline level.
     */
    public static int supportedLevel(
            Map<String, Integer> supportedFeatures, ClusterFeature feature) {
        Integer level = supportedFeatures.get(feature.featureName());
        return level != null ? level : feature.baselineLevel();
    }

    /**
     * Returns the features whose finalized level the registered supported features are below,
     * described as {@code name (supports x, finalized y)}. Empty if the server supports every
     * finalized level.
     */
    public static List<String> unsupportedFeatures(
            Map<String, Integer> supportedFeatures, ClusterFeatureLevels finalized) {
        List<String> unsupported = new ArrayList<>();
        for (ClusterFeature feature : ClusterFeature.values()) {
            int supported = supportedLevel(supportedFeatures, feature);
            int level = finalized.level(feature);
            if (supported < level) {
                unsupported.add(
                        String.format(
                                "%s (supports %s, finalized %s)",
                                feature.featureName(), supported, level));
            }
        }
        return unsupported;
    }

    /**
     * Returns the stored finalized levels that this build does not support: levels above its latest
     * level, and features it does not know at all. A stored feature is always above its baseline,
     * so an unknown one cannot be supported. Empty if this build supports all of them.
     */
    public static List<String> unsupportedByThisBuild(Map<String, Integer> storedLevels) {
        List<String> unsupported =
                unsupportedFeatures(
                        supportedFeatures(), ClusterFeatureLevels.fromMap(storedLevels));
        for (String unknown : ClusterFeatureLevels.unknownFeatures(storedLevels)) {
            unsupported.add(
                    String.format(
                            "%s (unknown to this version, finalized %s)",
                            unknown, storedLevels.get(unknown)));
        }
        return unsupported;
    }

    /**
     * Reads the finalized levels of the cluster from ZooKeeper. Every feature is at its baseline
     * while the cluster has not stored any levels yet.
     */
    public static ClusterFeatureLevels finalizedLevels(ZooKeeperClient zkClient) throws Exception {
        Optional<VersionedData<Map<String, Integer>>> stored = zkClient.getClusterFeatureLevels();
        return stored.isPresent()
                ? ClusterFeatureLevels.fromMap(stored.get().data())
                : ClusterFeatureLevels.baseline();
    }

    /**
     * Fails if the cluster has finalized a feature level this server does not support, for example
     * when a server of an older version starts after the cluster enabled a newer log record batch
     * format. The server could not read the data written at that level, so it must not start and
     * join the cluster.
     */
    public static void checkSupportsFinalizedFeatures(ZooKeeperClient zkClient) throws Exception {
        Optional<VersionedData<Map<String, Integer>>> stored = zkClient.getClusterFeatureLevels();
        if (!stored.isPresent()) {
            return;
        }
        List<String> unsupported = unsupportedByThisBuild(stored.get().data());
        if (!unsupported.isEmpty()) {
            throw new IllegalConfigurationException(
                    String.format(
                            "This server does not support the finalized cluster features %s. "
                                    + "Upgrade it to a version that supports them.",
                            unsupported));
        }
    }
}
