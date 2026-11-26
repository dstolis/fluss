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

package org.apache.fluss.rpc.protocol.feature;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.PbClusterFeatureLevel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** An immutable snapshot of the finalized level of every {@link ClusterFeature}. */
@Internal
public final class ClusterFeatureLevels {

    private final Map<ClusterFeature, Integer> levels;

    private ClusterFeatureLevels(Map<ClusterFeature, Integer> levels) {
        this.levels = Collections.unmodifiableMap(levels);
    }

    /**
     * Reads finalized levels from a map of feature name to level, as stored for the cluster. A
     * feature that is not in the map is at its baseline. Features this build does not know are
     * ignored, see {@link #unknownFeatures(Map)}.
     */
    public static ClusterFeatureLevels fromMap(Map<String, Integer> levelsByName) {
        Map<ClusterFeature, Integer> levels = new EnumMap<>(ClusterFeature.class);
        for (ClusterFeature feature : ClusterFeature.values()) {
            Integer level = levelsByName.get(feature.featureName());
            levels.put(
                    feature,
                    level == null
                            ? feature.baselineLevel()
                            : Math.max(feature.baselineLevel(), level));
        }
        return new ClusterFeatureLevels(levels);
    }

    /**
     * Returns the names in the given map of finalized levels that this build does not know. A
     * stored feature is above its baseline, so a build that does not know it cannot support it.
     */
    public static List<String> unknownFeatures(Map<String, Integer> levelsByName) {
        List<String> unknown = new ArrayList<>();
        for (String name : levelsByName.keySet()) {
            if (ClusterFeature.forName(name) == null) {
                unknown.add(name);
            }
        }
        Collections.sort(unknown);
        return unknown;
    }

    /** Every feature at the latest level this build supports. */
    public static ClusterFeatureLevels latest() {
        Map<ClusterFeature, Integer> levels = new EnumMap<>(ClusterFeature.class);
        for (ClusterFeature feature : ClusterFeature.values()) {
            levels.put(feature, feature.latestLevel());
        }
        return new ClusterFeatureLevels(levels);
    }

    /**
     * Reads the finalized levels a server reported in its {@code ApiVersions} response. A feature
     * the server did not report is at its baseline, which covers servers that do not know the
     * feature. Features this build does not know are ignored.
     */
    public static ClusterFeatureLevels fromReported(List<PbClusterFeatureLevel> reported) {
        Map<ClusterFeature, Integer> levels = new EnumMap<>(ClusterFeature.class);
        for (ClusterFeature feature : ClusterFeature.values()) {
            int level = feature.baselineLevel();
            for (PbClusterFeatureLevel reportedLevel : reported) {
                if (feature.featureName().equals(reportedLevel.getName())) {
                    level = Math.max(level, reportedLevel.getLevel());
                }
            }
            levels.put(feature, level);
        }
        return new ClusterFeatureLevels(levels);
    }

    /**
     * Reports these levels in the {@code ApiVersions} response of a server, so clients learn which
     * levels the cluster has finalized.
     */
    public void reportIn(ApiVersionsResponse response) {
        levels.forEach(
                (feature, level) ->
                        response.addFinalizedFeature()
                                .setName(feature.featureName())
                                .setLevel(level));
    }

    /** Every feature at its baseline level, which is the state of a cluster that enabled none. */
    public static ClusterFeatureLevels baseline() {
        Map<ClusterFeature, Integer> levels = new EnumMap<>(ClusterFeature.class);
        for (ClusterFeature feature : ClusterFeature.values()) {
            levels.put(feature, feature.baselineLevel());
        }
        return new ClusterFeatureLevels(levels);
    }

    /**
     * Returns the levels as a map of feature name to level, leaving out the features at their
     * baseline. This is the form in which the levels are stored for the cluster: a stored feature
     * is always above its baseline, so a server that does not know its name does not support it.
     */
    public Map<String, Integer> toMap() {
        Map<String, Integer> levelsByName = new TreeMap<>();
        levels.forEach(
                (feature, level) -> {
                    if (level > feature.baselineLevel()) {
                        levelsByName.put(feature.featureName(), level);
                    }
                });
        return levelsByName;
    }

    /** Returns a copy with the given feature at the given level. */
    public ClusterFeatureLevels with(ClusterFeature feature, int level) {
        Map<ClusterFeature, Integer> copy = new EnumMap<>(levels);
        copy.put(feature, level);
        return new ClusterFeatureLevels(copy);
    }

    /** Returns the higher level of every feature of this and the other levels. */
    public ClusterFeatureLevels max(ClusterFeatureLevels other) {
        Map<ClusterFeature, Integer> copy = new EnumMap<>(levels);
        for (ClusterFeature feature : ClusterFeature.values()) {
            copy.put(feature, Math.max(level(feature), other.level(feature)));
        }
        return new ClusterFeatureLevels(copy);
    }

    /** Whether every feature is at the level of the other levels or higher. */
    public boolean isAtLeast(ClusterFeatureLevels other) {
        for (ClusterFeature feature : ClusterFeature.values()) {
            if (level(feature) < other.level(feature)) {
                return false;
            }
        }
        return true;
    }

    /** The finalized level of the given feature. */
    public int level(ClusterFeature feature) {
        return levels.get(feature);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return levels.equals(((ClusterFeatureLevels) o).levels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(levels);
    }

    @Override
    public String toString() {
        return "ClusterFeatureLevels" + levels;
    }
}
