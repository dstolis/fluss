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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Describes every cluster feature, with the feature epoch at which their finalized levels were
 * read. The epoch goes up by one with every raise. Pass it to {@link
 * Admin#updateClusterFeatures(java.util.Map, int, boolean)} so that the raise is rejected if
 * another raise happened in between. It also lists the levels every registered tablet server has
 * applied, which show whether a raise has reached all of them.
 *
 * @since 1.1
 */
@PublicEvolving
public final class ClusterFeaturesDescription {

    private final int epoch;
    private final Collection<ClusterFeatureDescription> features;
    private final Collection<TabletServerFeatureProgress> tabletServers;

    public ClusterFeaturesDescription(
            int epoch,
            Collection<ClusterFeatureDescription> features,
            Collection<TabletServerFeatureProgress> tabletServers) {
        this.epoch = epoch;
        this.features = Collections.unmodifiableList(new ArrayList<>(features));
        this.tabletServers = Collections.unmodifiableList(new ArrayList<>(tabletServers));
    }

    /** The feature epoch of the finalized levels. */
    public int epoch() {
        return epoch;
    }

    /** The description of every feature. */
    public Collection<ClusterFeatureDescription> features() {
        return features;
    }

    /** The levels every registered tablet server has applied, in tablet server id order. */
    public Collection<TabletServerFeatureProgress> tabletServers() {
        return tabletServers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterFeaturesDescription that = (ClusterFeaturesDescription) o;
        return epoch == that.epoch
                && features.equals(that.features)
                && tabletServers.equals(that.tabletServers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, features, tabletServers);
    }

    @Override
    public String toString() {
        return "ClusterFeaturesDescription{epoch="
                + epoch
                + ", features="
                + features
                + ", tabletServers="
                + tabletServers
                + '}';
    }
}
