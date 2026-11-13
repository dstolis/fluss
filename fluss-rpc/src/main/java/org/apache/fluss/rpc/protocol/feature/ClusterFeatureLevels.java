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
import org.apache.fluss.config.Configuration;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/** An immutable snapshot of the finalized level of every {@link ClusterFeature}. */
@Internal
public final class ClusterFeatureLevels {

    private final Map<ClusterFeature, Integer> levels;

    private ClusterFeatureLevels(Map<ClusterFeature, Integer> levels) {
        this.levels = Collections.unmodifiableMap(levels);
    }

    /** Reads the finalized levels from the cluster configs. */
    public static ClusterFeatureLevels fromConfig(Configuration conf) {
        Map<ClusterFeature, Integer> levels = new EnumMap<>(ClusterFeature.class);
        for (ClusterFeature feature : ClusterFeature.values()) {
            levels.put(feature, conf.get(feature.configOption()));
        }
        return new ClusterFeatureLevels(levels);
    }

    /** Every feature at its baseline level, which is the state of a cluster that enabled none. */
    public static ClusterFeatureLevels baseline() {
        Map<ClusterFeature, Integer> levels = new EnumMap<>(ClusterFeature.class);
        for (ClusterFeature feature : ClusterFeature.values()) {
            levels.put(feature, feature.baselineLevel());
        }
        return new ClusterFeatureLevels(levels);
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
