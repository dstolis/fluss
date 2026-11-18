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
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.server.utils.FatalErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Follows the finalized cluster feature levels in ZooKeeper on a tablet server and applies every
 * change to its {@link FinalizedClusterFeatures}. The coordinator only raises a level that every
 * registered server supports, so a level this server does not support means it must not serve data,
 * and is reported as a fatal error.
 */
@Internal
public class ClusterFeaturesWatcher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterFeaturesWatcher.class);

    private final CuratorCache curatorCache;
    private final FinalizedClusterFeatures clusterFeatures;
    private final FatalErrorHandler fatalErrorHandler;

    public ClusterFeaturesWatcher(
            ZooKeeperClient zkClient,
            FinalizedClusterFeatures clusterFeatures,
            FatalErrorHandler fatalErrorHandler) {
        this.clusterFeatures = clusterFeatures;
        this.fatalErrorHandler = fatalErrorHandler;
        this.curatorCache =
                CuratorCache.build(
                        zkClient.getCuratorClient(),
                        ZkData.ClusterFeaturesZNode.path(),
                        CuratorCache.Options.SINGLE_NODE_CACHE);
        this.curatorCache.listenable().addListener(new ClusterFeaturesListener());
    }

    /** Starts watching. The current levels are applied as the first event. */
    public void start() {
        curatorCache.start();
    }

    @Override
    public void close() {
        curatorCache.close();
    }

    private final class ClusterFeaturesListener implements CuratorCacheListener {

        @Override
        public void event(Type type, ChildData oldData, ChildData newData) {
            if (type == Type.NODE_DELETED || newData == null || newData.getData().length == 0) {
                // the levels are never removed, and a removed node must not lower them
                return;
            }
            try {
                Map<String, Integer> storedLevels =
                        ZkData.ClusterFeaturesZNode.decode(newData.getData());
                List<String> unsupported = ClusterFeatures.unsupportedByThisBuild(storedLevels);
                if (!unsupported.isEmpty()) {
                    fatalErrorHandler.onFatalError(
                            new IllegalStateException(
                                    String.format(
                                            "The cluster finalized the cluster features %s, which "
                                                    + "this tablet server does not support.",
                                            unsupported)));
                    return;
                }
                clusterFeatures.apply(ClusterFeatureLevels.fromMap(storedLevels));
            } catch (Throwable t) {
                LOG.error("Failed to apply the finalized cluster feature levels.", t);
            }
        }
    }
}
