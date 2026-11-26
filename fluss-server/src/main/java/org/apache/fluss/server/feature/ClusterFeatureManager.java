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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.InvalidClusterFeatureUpdateException;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.rpc.protocol.feature.SupportedLevelRange;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.AppliedClusterFeatures;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Manages the finalized cluster feature levels on the leading coordinator: it creates them when a
 * cluster has none, describes them, and raises them.
 *
 * <p>The levels are stored in their own ZooKeeper node, which only the leading coordinator writes.
 * Every write is fenced on the coordinator epoch, so a coordinator that lost leadership cannot
 * write, and a raise is conditional on the version of the node it was validated against, so it
 * cannot overwrite a newer state. Together this keeps a level from ever going down.
 *
 * <p>The version of that node is the feature epoch: it goes up by one with every raise, and never
 * otherwise. An operator can pass the epoch they described to a raise, which is then rejected if
 * another raise happened in between.
 *
 * <p>Like a Kafka controller, which only counts the brokers whose registration it accepted, it only
 * counts the tablet servers the coordinator has admitted to the live tablet servers. A server that
 * registered but was kept out, because it does not support a finalized level, cannot serve data, so
 * it neither lowers the supported levels nor blocks a raise. Every registered coordinator counts.
 *
 * <p>The methods must run on the coordinator event thread, which also admits tablet servers. A
 * server that registers while a level is raised is then either seen by the validation, or checked
 * against the raised level when it is admitted.
 */
@Internal
public class ClusterFeatureManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterFeatureManager.class);

    private final ZooKeeperClient zkClient;

    public ClusterFeatureManager(ZooKeeperClient zkClient) {
        this.zkClient = zkClient;
    }

    /**
     * Makes sure the cluster has stored feature levels, and returns them. Called when this
     * coordinator takes leadership, before it creates anything else in ZooKeeper.
     *
     * <p>A cluster without stored levels is either new, or was upgraded from a version without
     * cluster features. A new cluster, one whose ZooKeeper holds no database yet, starts with every
     * feature at the latest level this coordinator supports. An upgraded cluster starts with every
     * feature at its baseline, because its servers may not all be upgraded yet, and the operator
     * raises the levels afterwards.
     */
    public ClusterFeatureLevels initialize(int coordinatorEpochZkVersion) throws Exception {
        Optional<VersionedData<Map<String, Integer>>> stored = zkClient.getClusterFeatureLevels();
        if (stored.isPresent()) {
            return ClusterFeatureLevels.fromMap(stored.get().data());
        }
        boolean newCluster = zkClient.listDatabases().isEmpty();
        ClusterFeatureLevels initial =
                newCluster ? ClusterFeatureLevels.latest() : ClusterFeatureLevels.baseline();
        checkCanStartAt(initial, ClusterFeatures.supportedFeatures());
        checkNoTabletServerAppliedMore(initial, zkClient.getAllAppliedClusterFeatures());
        try {
            zkClient.createClusterFeatureLevels(initial.toMap(), coordinatorEpochZkVersion);
        } catch (KeeperException.NodeExistsException e) {
            return ClusterFeatures.finalizedLevels(zkClient);
        }
        LOG.info(
                "Initialized the cluster feature levels of {} cluster to {}.",
                newCluster ? "a new" : "an upgraded",
                initial);
        return initial;
    }

    /**
     * Fails if this coordinator does not support the levels a cluster without stored levels would
     * start at. That happens to an upgraded cluster when this version no longer supports the
     * baseline of a feature: its servers can't run at the baseline, and the level can't be raised
     * while they are offline, so nothing is stored and the operator is told what to do instead.
     */
    @VisibleForTesting
    static void checkCanStartAt(
            ClusterFeatureLevels initial, Map<String, SupportedLevelRange> supportedFeatures) {
        List<String> unsupported = ClusterFeatures.unsupportedFeatures(supportedFeatures, initial);
        if (!unsupported.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot initialize the cluster feature levels at %s, because this "
                                    + "coordinator does not support %s. A cluster that ran a "
                                    + "version without cluster features starts at the baseline "
                                    + "levels: upgrade it to a version that supports them first, "
                                    + "raise the levels, then upgrade to this version.",
                            initial, unsupported));
        }
    }

    /**
     * Fails if a running tablet server reports a level above the given initial levels. That happens
     * when the stored levels of a cluster were lost, for example by a ZooKeeper restore: starting
     * again at the baseline would lower a level that data was written at. The operator must restore
     * the stored levels instead.
     */
    @VisibleForTesting
    static void checkNoTabletServerAppliedMore(
            ClusterFeatureLevels initial, Map<Integer, AppliedClusterFeatures> appliedByServer) {
        Map<Integer, ClusterFeatureLevels> higher = new TreeMap<>();
        ClusterFeatureLevels highest = initial;
        for (Map.Entry<Integer, AppliedClusterFeatures> entry : appliedByServer.entrySet()) {
            ClusterFeatureLevels applied = ClusterFeatureLevels.fromMap(entry.getValue().levels());
            if (!initial.isAtLeast(applied)) {
                higher.put(entry.getKey(), applied);
                highest = highest.max(applied);
            }
        }
        if (!higher.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "The cluster feature levels are not stored at %s under the ZooKeeper "
                                    + "root, but tablet servers %s applied higher levels than "
                                    + "%s. The node was lost, for example by a ZooKeeper restore, "
                                    + "and starting again would lower a level. Restore the node, "
                                    + "for example to %s, then restart the coordinators.",
                            ZkData.ClusterFeaturesZNode.path(),
                            higher,
                            initial,
                            ZkData.ClusterFeaturesZNode.toJson(highest)));
        }
    }

    /**
     * The finalized level of every feature, with the feature epoch they were read at, which goes up
     * by one with every raise.
     */
    public VersionedData<ClusterFeatureLevels> finalizedState() throws Exception {
        VersionedData<Map<String, Integer>> stored = storedLevels();
        return new VersionedData<>(ClusterFeatureLevels.fromMap(stored.data()), stored.zkVersion());
    }

    /**
     * The highest level of every feature that this coordinator, every registered coordinator and
     * every admitted tablet server support, which is the level the feature can be raised to right
     * now.
     *
     * @param admittedTabletServers the tablet servers the coordinator has admitted
     */
    public ClusterFeatureLevels supportedLevels(Set<Integer> admittedTabletServers)
            throws Exception {
        ClusterFeatureLevels supported = ClusterFeatureLevels.latest();
        for (Map<String, SupportedLevelRange> serverFeatures :
                serverFeatures(admittedTabletServers).values()) {
            for (ClusterFeature feature : ClusterFeature.values()) {
                int level = ClusterFeatures.supportedRange(serverFeatures, feature).max();
                if (level < supported.level(feature)) {
                    supported = supported.with(feature, level);
                }
            }
        }
        return supported;
    }

    /**
     * Raises the finalized level of the given features, all of them or none, and returns the
     * resulting levels and epoch. With {@code validateOnly}, the change is validated but not made.
     *
     * <p>A raise to the levels the cluster already has succeeds whatever the expected epoch, so a
     * client can safely retry a raise whose response it did not receive.
     *
     * @param expectedEpoch the feature epoch the caller expects, or null to raise whatever the
     *     current epoch is
     * @param admittedTabletServers the tablet servers the coordinator has admitted
     * @throws InvalidClusterFeatureUpdateException if a feature is unknown, a level is lowered or
     *     out of range, the feature epoch is not the expected one, a tablet server that holds
     *     replicas is offline, or a registered coordinator or admitted tablet server does not
     *     support a level
     * @throws NotCoordinatorLeaderException if this coordinator lost leadership, in which case
     *     nothing was changed
     */
    public VersionedData<ClusterFeatureLevels> update(
            Map<String, Integer> requestedLevels,
            boolean validateOnly,
            @Nullable Integer expectedEpoch,
            int coordinatorEpochZkVersion,
            Set<Integer> admittedTabletServers)
            throws Exception {
        VersionedData<Map<String, Integer>> stored = storedLevels();
        int epoch = stored.zkVersion();
        ClusterFeatureLevels current = ClusterFeatureLevels.fromMap(stored.data());
        ClusterFeatureLevels updated = current;
        for (Map.Entry<String, Integer> requested : requestedLevels.entrySet()) {
            ClusterFeature feature = ClusterFeature.forName(requested.getKey());
            if (feature == null) {
                throw new InvalidClusterFeatureUpdateException(
                        String.format(
                                "Unknown cluster feature '%s', the cluster features are %s.",
                                requested.getKey(), ClusterFeatures.supportedFeatures().keySet()));
            }
            int level = requested.getValue();
            int currentLevel = current.level(feature);
            if (level < currentLevel) {
                throw new InvalidClusterFeatureUpdateException(
                        String.format(
                                "Cannot lower cluster feature '%s' from %s to %s, because data "
                                        + "written at level %s may not be readable at a lower level.",
                                feature.featureName(), currentLevel, level, currentLevel));
            }
            if (level > feature.latestLevel()) {
                throw new InvalidClusterFeatureUpdateException(
                        String.format(
                                "Invalid level %s for cluster feature '%s', this coordinator "
                                        + "supports levels %s.",
                                level, feature.featureName(), feature.supportedRange()));
            }
            updated = updated.with(feature, level);
        }
        if (updated.equals(current)) {
            return new VersionedData<>(current, epoch);
        }
        if (expectedEpoch != null && expectedEpoch != epoch) {
            throw new InvalidClusterFeatureUpdateException(
                    String.format(
                            "Cannot raise the cluster feature levels to %s, because the feature "
                                    + "epoch is %s, not the expected %s: the levels changed to %s "
                                    + "since. Describe them again and retry.",
                            updated, epoch, expectedEpoch, current));
        }

        checkEveryServerSupports(current, updated, admittedTabletServers);
        if (validateOnly) {
            return new VersionedData<>(updated, epoch);
        }
        try {
            zkClient.updateClusterFeatureLevels(updated.toMap(), epoch, coordinatorEpochZkVersion);
        } catch (KeeperException.BadVersionException e) {
            throw new NotCoordinatorLeaderException(
                    "The cluster feature levels were not changed, because this coordinator is no "
                            + "longer the leader. Retry with the leading coordinator.");
        }
        LOG.info(
                "Raised the finalized cluster feature levels from {} to {}, feature epoch {}.",
                current,
                updated,
                epoch + 1);
        // a successful conditional write moves the node to the next version
        return new VersionedData<>(updated, epoch + 1);
    }

    private VersionedData<Map<String, Integer>> storedLevels() throws Exception {
        Optional<VersionedData<Map<String, Integer>>> stored = zkClient.getClusterFeatureLevels();
        if (!stored.isPresent()) {
            throw new NotCoordinatorLeaderException(
                    "The cluster feature levels are not initialized yet, retry later.");
        }
        return stored.get();
    }

    private void checkEveryServerSupports(
            ClusterFeatureLevels current,
            ClusterFeatureLevels updated,
            Set<Integer> admittedTabletServers)
            throws Exception {
        // A server that holds replicas and is not registered may come back with any version, and
        // its support cannot be checked. That includes a server the coordinator still counts as
        // admitted because it has not processed its exit yet. One that is registered but was not
        // admitted is already kept out until it is upgraded, and checked against the raised level
        // when it is.
        Set<Integer> registered = new HashSet<>();
        for (int serverId : zkClient.getSortedTabletServerList()) {
            registered.add(serverId);
        }
        List<Integer> offline = new ArrayList<>();
        for (int serverId : zkClient.getTabletServersWithReplicas()) {
            if (!registered.contains(serverId)) {
                offline.add(serverId);
            }
        }
        if (!offline.isEmpty()) {
            Collections.sort(offline);
            throw new InvalidClusterFeatureUpdateException(
                    String.format(
                            "Cannot raise the cluster feature levels to %s, because tablet servers "
                                    + "%s hold replicas but are offline, so their support cannot be "
                                    + "checked. Bring them back or move their replicas first.",
                            updated, offline));
        }

        Map<String, Map<String, SupportedLevelRange>> serverFeatures =
                serverFeatures(admittedTabletServers);
        for (ClusterFeature feature : ClusterFeature.values()) {
            int level = updated.level(feature);
            if (level == current.level(feature)) {
                continue;
            }
            List<String> unsupported = new ArrayList<>();
            serverFeatures.forEach(
                    (server, features) -> {
                        if (!ClusterFeatures.supportedRange(features, feature).contains(level)) {
                            unsupported.add(server);
                        }
                    });
            if (!unsupported.isEmpty()) {
                Collections.sort(unsupported);
                throw new InvalidClusterFeatureUpdateException(
                        String.format(
                                "Cannot raise cluster feature '%s' to %s, because %s do not "
                                        + "support it. Upgrade them first.",
                                feature.featureName(), level, unsupported));
            }
        }
    }

    /**
     * The supported features of every admitted tablet server and registered coordinator, keyed by a
     * name for messages.
     */
    private Map<String, Map<String, SupportedLevelRange>> serverFeatures(
            Set<Integer> admittedTabletServers) throws Exception {
        Map<String, Map<String, SupportedLevelRange>> serverFeatures = new TreeMap<>();
        int[] admitted = admittedTabletServers.stream().mapToInt(Integer::intValue).toArray();
        zkClient.getTabletServers(admitted)
                .forEach(
                        (serverId, registration) ->
                                serverFeatures.put(
                                        "tablet server " + serverId,
                                        registration.getSupportedFeatures()));
        for (CoordinatorAddress coordinator :
                zkClient.getCoordinatorServers(zkClient.getCoordinatorServerList()).values()) {
            serverFeatures.put(
                    "coordinator " + coordinator.getId(), coordinator.getSupportedFeatures());
        }
        return serverFeatures;
    }
}
