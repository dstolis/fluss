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

/**
 * The finalized level of every cluster feature, as applied on this tablet server. It follows the
 * cluster configs, which the coordinator only changes once every tablet server supports the new
 * level.
 */
@Internal
public class FinalizedClusterFeatures implements ServerReconfigurable {

    private volatile ClusterFeatureLevels levels;

    public FinalizedClusterFeatures(Configuration conf) {
        this.levels = ClusterFeatureLevels.fromConfig(conf);
    }

    /** The levels applied on this server. */
    public ClusterFeatureLevels levels() {
        return levels;
    }

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        for (ClusterFeature feature : ClusterFeature.values()) {
            int level = newConfig.get(feature.configOption());
            if (level > feature.latestLevel()) {
                throw new ConfigException(
                        String.format(
                                "This tablet server supports cluster feature '%s' up to level %s, "
                                        + "but the cluster finalized level %s.",
                                feature.featureName(), feature.latestLevel(), level));
            }
        }
    }

    @Override
    public void reconfigure(Configuration newConfig) {
        levels = ClusterFeatureLevels.fromConfig(newConfig);
    }
}
