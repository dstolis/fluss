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
import org.apache.fluss.exception.UnreadableClusterFeatureLevelsException;
import org.apache.fluss.rpc.protocol.feature.ApiFeatureContracts;
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.rpc.protocol.feature.SupportedLevelRange;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.utils.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** The cluster features a server registers and checks, see {@link ClusterFeature}. */
@Internal
public final class ClusterFeatures {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterFeatures.class);

    private ClusterFeatures() {}

    /** The features this server supports, mapped to the range of supported levels. */
    public static Map<String, SupportedLevelRange> supportedFeatures() {
        Map<String, SupportedLevelRange> supported = new HashMap<>();
        for (ClusterFeature feature : ClusterFeature.values()) {
            supported.put(feature.featureName(), feature.supportedRange());
        }
        return Collections.unmodifiableMap(supported);
    }

    /**
     * Returns the levels of the feature in the supported features a server registered. Servers that
     * registered before the feature existed support only its baseline level.
     */
    public static SupportedLevelRange supportedRange(
            Map<String, SupportedLevelRange> supportedFeatures, ClusterFeature feature) {
        SupportedLevelRange range = supportedFeatures.get(feature.featureName());
        return range != null ? range : feature.baselineRange();
    }

    /**
     * Returns the features whose finalized level is outside the range the server registered,
     * described as {@code name (supports [min, max], finalized y)}. Empty if the server supports
     * every finalized level.
     */
    public static List<String> unsupportedFeatures(
            Map<String, SupportedLevelRange> supportedFeatures, ClusterFeatureLevels finalized) {
        List<String> unsupported = new ArrayList<>();
        for (ClusterFeature feature : ClusterFeature.values()) {
            SupportedLevelRange supported = supportedRange(supportedFeatures, feature);
            int level = finalized.level(feature);
            if (!supported.contains(level)) {
                unsupported.add(
                        String.format(
                                "%s (supports %s, finalized %s)",
                                feature.featureName(), supported, level));
            }
        }
        return unsupported;
    }

    /**
     * Returns the stored finalized levels that this build does not support: levels outside its
     * supported range, and features it does not know at all. A stored feature is always above its
     * baseline, so an unknown one cannot be supported. Empty if this build supports all of them.
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
     * Fails if the cluster has finalized a feature level this coordinator does not support, or if
     * the feature contracts of this build are inconsistent, see {@link #checkSupportedByThisBuild}.
     * Stored levels that can't be read don't fail this check. A tablet server checks the stored
     * levels in {@link #levelsToStartWith} instead.
     */
    public static void checkSupportsFinalizedFeatures(ZooKeeperClient zkClient) throws Exception {
        checkContracts();
        Optional<VersionedData<Map<String, Integer>>> stored;
        try {
            stored = zkClient.getClusterFeatureLevels();
        } catch (UnreadableClusterFeatureLevelsException e) {
            return;
        }
        if (stored.isPresent()) {
            checkSupportedByThisBuild(stored.get().data());
        }
    }

    /**
     * Fails if the stored levels contain a level this server does not support, for example when a
     * server of an older version starts after the cluster enabled a newer log record batch format.
     * The server could not read the data written at that level, so it must not start and join the
     * cluster.
     */
    private static void checkSupportedByThisBuild(Map<String, Integer> storedLevels) {
        List<String> unsupported = unsupportedByThisBuild(storedLevels);
        if (!unsupported.isEmpty()) {
            throw new IllegalConfigurationException(
                    String.format(
                            "This server does not support the finalized cluster features %s. "
                                    + "Upgrade it to a version that supports them.",
                            unsupported));
        }
    }

    /**
     * Fails if the feature contracts of this build are inconsistent, so that such a build does not
     * start, see {@link ApiFeatureContracts#validate}.
     */
    public static void checkContracts() {
        try {
            ApiFeatureContracts.validate();
        } catch (IllegalStateException e) {
            throw new IllegalStateException(
                    "The cluster feature contracts of this build are invalid: " + e.getMessage(),
                    e);
        }
    }

    /**
     * The levels a tablet server starts with, with their feature epoch: the stored levels, unless
     * this server does not support them, or they would lower a level the server applied before.
     * Data written at that level may be on its disks, so the server refuses to start and tells the
     * operator to restore the stored levels, which can only go down if their ZooKeeper node was
     * lost, rolled back or edited by hand.
     *
     * <p>If the stored levels can't be read, a server that applied levels before starts with those,
     * so a rolling restart does not turn into an outage while the node is repaired.
     *
     * @param storedLevels reads the stored levels and their epoch, absent if none are stored
     * @param appliedBefore the levels this server applied before it was restarted, if any
     */
    public static VersionedData<ClusterFeatureLevels> levelsToStartWith(
            SupplierWithException<Optional<VersionedData<Map<String, Integer>>>, Exception>
                    storedLevels,
            Optional<ClusterFeatureLevels> appliedBefore)
            throws Exception {
        Optional<VersionedData<Map<String, Integer>>> stored;
        try {
            stored = storedLevels.get();
        } catch (UnreadableClusterFeatureLevelsException e) {
            if (!appliedBefore.isPresent()) {
                throw e;
            }
            LOG.error(
                    "{} Starting with the levels this tablet server applied before, {}.",
                    e.getMessage(),
                    appliedBefore.get());
            return new VersionedData<>(appliedBefore.get(), FinalizedClusterFeatures.NO_EPOCH);
        }
        if (stored.isPresent()) {
            checkSupportedByThisBuild(stored.get().data());
        }
        ClusterFeatureLevels before = appliedBefore.orElse(ClusterFeatureLevels.baseline());
        ClusterFeatureLevels current =
                stored.isPresent()
                        ? ClusterFeatureLevels.fromMap(stored.get().data())
                        : ClusterFeatureLevels.baseline();
        if (!current.isAtLeast(before)) {
            throw new IllegalStateException(
                    String.format(
                            "This tablet server applied the cluster feature levels %s before, but "
                                    + "the levels stored at %s under the ZooKeeper root are %s. "
                                    + "Levels never go down, because data written at a level may "
                                    + "exist, so the stored levels were lost, rolled back or "
                                    + "edited. Restore the node to at least %s.",
                            before,
                            ZkData.ClusterFeaturesZNode.path(),
                            stored.isPresent() ? current : "missing",
                            ZkData.ClusterFeaturesZNode.toJson(before)));
        }
        return new VersionedData<>(
                current,
                stored.isPresent() ? stored.get().zkVersion() : FinalizedClusterFeatures.NO_EPOCH);
    }
}
