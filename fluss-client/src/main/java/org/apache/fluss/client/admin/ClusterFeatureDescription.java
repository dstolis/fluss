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

import java.util.Objects;

/**
 * Describes a cluster feature: a capability, such as a new data format, that the cluster turns on
 * once every server supports it. A feature has numbered levels. The finalized level is the one the
 * cluster has enabled, and it only goes up.
 *
 * @since 1.1
 */
@PublicEvolving
public final class ClusterFeatureDescription {

    private final String name;
    private final int finalizedLevel;
    private final int supportedLevel;

    public ClusterFeatureDescription(String name, int finalizedLevel, int supportedLevel) {
        this.name = name;
        this.finalizedLevel = finalizedLevel;
        this.supportedLevel = supportedLevel;
    }

    /** The name of the feature, for example {@code log.record-batch.version}. */
    public String name() {
        return name;
    }

    /** The level the cluster has enabled. */
    public int finalizedLevel() {
        return finalizedLevel;
    }

    /**
     * The highest level that every registered coordinator and tablet server supports, which is the
     * level the feature can be raised to right now.
     */
    public int supportedLevel() {
        return supportedLevel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterFeatureDescription that = (ClusterFeatureDescription) o;
        return finalizedLevel == that.finalizedLevel
                && supportedLevel == that.supportedLevel
                && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, finalizedLevel, supportedLevel);
    }

    @Override
    public String toString() {
        return "ClusterFeatureDescription{"
                + "name='"
                + name
                + '\''
                + ", finalizedLevel="
                + finalizedLevel
                + ", supportedLevel="
                + supportedLevel
                + '}';
    }
}
