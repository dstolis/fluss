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
import org.apache.fluss.exception.InvalidCoordinatorException;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.CoordinatorAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Optional;

/**
 * Keeps a tablet server from following an active coordinator that doesn't support the cluster
 * feature levels this server has applied.
 *
 * <p>Coordinators of versions without cluster features register no supported levels, so they count
 * as supporting only the baseline of every feature, like such tablet servers. A raise is refused
 * while such a coordinator is registered. But one can still start afterwards and win the election,
 * and it would then admit tablet servers of its own version and assign them replicas holding data
 * they cannot read. Its requests are therefore refused, so it cannot move replicas or change the
 * metadata of this server until a coordinator that supports the levels leads again.
 *
 * <p>A request is attributed to the active coordinator registered in ZooKeeper only if the
 * coordinator epoch stored there is the request's. The registration is read once per coordinator
 * epoch and applied levels. If ZooKeeper cannot be read, the request is let through, as before.
 */
@Internal
@ThreadSafe
public class ActiveCoordinatorFeatureCheck {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveCoordinatorFeatureCheck.class);

    private final ZooKeeperClient zkClient;
    private final FinalizedClusterFeatures clusterFeatures;

    @GuardedBy("this")
    private int checkedEpoch = Integer.MIN_VALUE;

    @GuardedBy("this")
    @Nullable
    private ClusterFeatureLevels checkedLevels;

    /** Why the coordinator of {@link #checkedEpoch} is refused, null if it is supported. */
    @GuardedBy("this")
    @Nullable
    private String refusal;

    public ActiveCoordinatorFeatureCheck(
            ZooKeeperClient zkClient, FinalizedClusterFeatures clusterFeatures) {
        this.zkClient = zkClient;
        this.clusterFeatures = clusterFeatures;
    }

    /**
     * Fails with an {@link InvalidCoordinatorException} if the coordinator that sent a request of
     * the given coordinator epoch doesn't support the levels applied on this server.
     */
    public synchronized void check(int coordinatorEpoch, String requestName) {
        ClusterFeatureLevels applied = clusterFeatures.levels();
        if (coordinatorEpoch != checkedEpoch || !applied.equals(checkedLevels)) {
            if (!evaluate(coordinatorEpoch, applied)) {
                return;
            }
        }
        if (refusal != null) {
            LOG.warn("Refusing the {} request: {}", requestName, refusal);
            throw new InvalidCoordinatorException(refusal);
        }
    }

    /** Evaluates the coordinator of the epoch, false if it cannot be identified. */
    private boolean evaluate(int coordinatorEpoch, ClusterFeatureLevels applied) {
        Optional<CoordinatorAddress> active;
        try {
            ZkEpoch storedEpoch = zkClient.getCurrentEpoch();
            if (storedEpoch.getCoordinatorEpoch() != coordinatorEpoch) {
                // another coordinator leads by now, the coordinator epoch fences this one
                return false;
            }
            active = zkClient.getCoordinatorLeaderAddress();
        } catch (Exception e) {
            LOG.warn("Failed to read the active coordinator to check its cluster features.", e);
            return false;
        }
        if (!active.isPresent()) {
            return false;
        }
        List<String> unsupported =
                ClusterFeatures.unsupportedFeatures(active.get().getSupportedFeatures(), applied);
        checkedEpoch = coordinatorEpoch;
        checkedLevels = applied;
        if (unsupported.isEmpty()) {
            refusal = null;
        } else {
            refusal =
                    String.format(
                            "the active coordinator %s of coordinator epoch %s does not support the "
                                    + "finalized cluster features %s. Upgrade it to a version that "
                                    + "supports them.",
                            active.get().getId(), coordinatorEpoch, unsupported);
            LOG.error("This tablet server refuses the requests of {}", refusal);
        }
        return true;
    }
}
