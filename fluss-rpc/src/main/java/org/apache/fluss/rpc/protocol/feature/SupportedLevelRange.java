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

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The levels of a {@link ClusterFeature} that a server supports, from a minimum to a maximum, both
 * included. The maximum decides whether the cluster can raise the feature. The minimum lets a later
 * release drop support for an old level: a server whose minimum is above the finalized level cannot
 * join the cluster.
 */
@Internal
public final class SupportedLevelRange {

    private final int min;
    private final int max;

    public SupportedLevelRange(int min, int max) {
        checkArgument(
                min <= max,
                "The minimum supported level %s must not be above the maximum %s.",
                min,
                max);
        this.min = min;
        this.max = max;
    }

    /** The lowest supported level. */
    public int min() {
        return min;
    }

    /** The highest supported level. */
    public int max() {
        return max;
    }

    /** Whether the given level is supported. */
    public boolean contains(int level) {
        return min <= level && level <= max;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SupportedLevelRange that = (SupportedLevelRange) o;
        return min == that.min && max == that.max;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max);
    }

    @Override
    public String toString() {
        return "[" + min + ", " + max + "]";
    }
}
