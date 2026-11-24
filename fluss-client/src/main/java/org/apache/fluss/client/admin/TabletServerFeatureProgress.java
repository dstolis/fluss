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

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The cluster feature levels a tablet server has applied. A raise returns once the new levels are
 * stored, and the tablet servers apply them shortly after. A server has applied a raise once its
 * applied epoch reaches the feature epoch of {@link ClusterFeaturesDescription#epoch()}.
 *
 * @since 1.1
 */
@PublicEvolving
public final class TabletServerFeatureProgress {

    private final int tabletServerId;
    @Nullable private final Integer appliedEpoch;
    private final Map<String, Integer> appliedLevels;

    public TabletServerFeatureProgress(
            int tabletServerId,
            @Nullable Integer appliedEpoch,
            Map<String, Integer> appliedLevels) {
        this.tabletServerId = tabletServerId;
        this.appliedEpoch = appliedEpoch;
        this.appliedLevels = Collections.unmodifiableMap(new TreeMap<>(appliedLevels));
    }

    /** The id of the tablet server. */
    public int tabletServerId() {
        return tabletServerId;
    }

    /**
     * The feature epoch of the levels the server has applied, -1 if the cluster had stored no
     * levels when it applied them, or null if the server reported nothing, for example because it
     * runs a version without cluster features.
     */
    @Nullable
    public Integer appliedEpoch() {
        return appliedEpoch;
    }

    /** The level the server has applied for every feature, by feature name. */
    public Map<String, Integer> appliedLevels() {
        return appliedLevels;
    }

    /** Whether the server has applied the levels of the given feature epoch, or later ones. */
    public boolean hasApplied(int featureEpoch) {
        return appliedEpoch != null && appliedEpoch >= featureEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TabletServerFeatureProgress that = (TabletServerFeatureProgress) o;
        return tabletServerId == that.tabletServerId
                && Objects.equals(appliedEpoch, that.appliedEpoch)
                && appliedLevels.equals(that.appliedLevels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tabletServerId, appliedEpoch, appliedLevels);
    }

    @Override
    public String toString() {
        return "TabletServerFeatureProgress{tabletServerId="
                + tabletServerId
                + ", appliedEpoch="
                + appliedEpoch
                + ", appliedLevels="
                + appliedLevels
                + '}';
    }
}
