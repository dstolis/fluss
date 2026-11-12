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
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.server.zk.ZooKeeperClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The cluster features a tablet server registers, see {@link ClusterFeature}. */
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
     * Returns the highest level of the feature in the supported features a tablet server
     * registered. Servers that registered before the feature existed support its baseline level.
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
     * Reads the finalized levels as the coordinator sees them: the static config overlaid with the
     * cluster configs stored in ZooKeeper.
     */
    public static ClusterFeatureLevels finalizedLevels(
            Configuration staticConf, ZooKeeperClient zkClient) throws Exception {
        Configuration conf = new Configuration(staticConf);
        conf.addAll(Configuration.fromMap(zkClient.fetchEntityConfig()));
        return ClusterFeatureLevels.fromConfig(conf);
    }
}
