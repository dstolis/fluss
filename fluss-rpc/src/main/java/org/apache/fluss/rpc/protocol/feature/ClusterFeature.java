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
import org.apache.fluss.record.LogRecordBatchFormat;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A capability that a cluster turns on once every server supports it, such as a new log record
 * batch format. Each feature has numbered levels. Every server registers the range of levels it
 * supports, and the cluster has one finalized level per feature, which the coordinator only raises
 * once every registered server supports it and never lowers.
 *
 * <p>Which data a reader of each API version can decode is declared in {@link ApiFeatureContracts}.
 */
@Internal
public enum ClusterFeature {
    /**
     * The log record batch format that writers may use. The levels are batch magic values: level 2
     * allows the formats before V3, and level 3 also allows {@link
     * LogRecordBatchFormat#LOG_MAGIC_VALUE_V3}.
     */
    LOG_RECORD_BATCH_VERSION(
            "log.record-batch.version",
            LogRecordBatchFormat.LOG_MAGIC_VALUE_V2,
            LogRecordBatchFormat.LOG_MAGIC_VALUE_V2,
            LogRecordBatchFormat.LOG_MAGIC_VALUE_V3);

    private final String featureName;
    private final int baselineLevel;
    private final int minimumLevel;
    private final int latestLevel;

    ClusterFeature(String featureName, int baselineLevel, int minimumLevel, int latestLevel) {
        checkArgument(
                baselineLevel <= minimumLevel && minimumLevel <= latestLevel,
                "The levels of cluster feature %s must be ordered baseline <= minimum <= latest.",
                featureName);
        this.featureName = featureName;
        this.baselineLevel = baselineLevel;
        this.minimumLevel = minimumLevel;
        this.latestLevel = latestLevel;
    }

    /** Returns the feature with the given name, or null if this build does not know it. */
    @Nullable
    public static ClusterFeature forName(String featureName) {
        for (ClusterFeature feature : values()) {
            if (feature.featureName.equals(featureName)) {
                return feature;
            }
        }
        return null;
    }

    /** The name under which servers register the level they support. */
    public String featureName() {
        return featureName;
    }

    /**
     * The level every server supports, which is also the level of servers that registered before
     * the feature existed.
     */
    public int baselineLevel() {
        return baselineLevel;
    }

    /**
     * The lowest level this build supports. A later release raises it to drop the code of an old
     * level, once every cluster is above it.
     */
    public int minimumLevel() {
        return minimumLevel;
    }

    /** The highest level this build supports. */
    public int latestLevel() {
        return latestLevel;
    }

    /** The levels this build supports. */
    public SupportedLevelRange supportedRange() {
        return new SupportedLevelRange(minimumLevel, latestLevel);
    }

    /**
     * The levels supported by a server that registered before the feature existed, or that did not
     * register it: only the baseline.
     */
    public SupportedLevelRange baselineRange() {
        return new SupportedLevelRange(baselineLevel, baselineLevel);
    }
}
