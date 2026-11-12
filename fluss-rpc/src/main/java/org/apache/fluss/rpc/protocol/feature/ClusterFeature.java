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
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.record.LogRecordBatchFormat;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A capability that a cluster turns on once every server supports it, such as a new log record
 * batch format. Each feature has numbered levels. A tablet server registers the highest level it
 * supports, and the cluster config {@code cluster.feature.<name>} holds the finalized level, which
 * the coordinator only raises once every registered tablet server supports it and never lowers.
 *
 * <p>Which API versions may exchange data of which level is declared in {@link
 * ApiFeatureContracts}.
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
            ConfigOptions.CLUSTER_FEATURE_LOG_RECORD_BATCH_VERSION,
            LogRecordBatchFormat.LOG_MAGIC_VALUE_V2,
            LogRecordBatchFormat.LOG_MAGIC_VALUE_V3);

    /** The prefix of the cluster config that holds the finalized level of a feature. */
    public static final String CONFIG_PREFIX = "cluster.feature.";

    private final String featureName;
    private final ConfigOption<Integer> configOption;
    private final int baselineLevel;
    private final int latestLevel;

    ClusterFeature(
            String featureName,
            ConfigOption<Integer> configOption,
            int baselineLevel,
            int latestLevel) {
        checkArgument(
                configOption.key().equals(CONFIG_PREFIX + featureName),
                "The config of cluster feature %s must be %s%s.",
                featureName,
                CONFIG_PREFIX,
                featureName);
        checkArgument(
                configOption.defaultValue() == baselineLevel,
                "The config of cluster feature %s must default to its baseline level.",
                featureName);
        this.featureName = featureName;
        this.configOption = configOption;
        this.baselineLevel = baselineLevel;
        this.latestLevel = latestLevel;
    }

    /** The name under which tablet servers register the level they support. */
    public String featureName() {
        return featureName;
    }

    /** The cluster config that holds the finalized level of this feature. */
    public ConfigOption<Integer> configOption() {
        return configOption;
    }

    /**
     * The level every server supports, which is also the level of servers that registered before
     * the feature existed.
     */
    public int baselineLevel() {
        return baselineLevel;
    }

    /** The highest level this build supports. */
    public int latestLevel() {
        return latestLevel;
    }
}
