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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.record.LogRecordBatchFormat;

import java.util.Collections;
import java.util.Map;

/**
 * Cluster features whose level is finalized for the whole cluster, separately from upgrading the
 * server binaries.
 *
 * <p>Every tablet server advertises the highest level it supports for each feature in its
 * registration. The finalized level is a dynamic cluster config that the coordinator only lets an
 * operator raise once every registered tablet server supports the new level, and never lower,
 * because data written at a higher level may not be readable at a lower one. Features with
 * cross-node compatibility requirements, such as a new log record batch format, must only be used
 * once the cluster has finalized the level that enables them.
 */
@Internal
public final class ClusterFeatures {

    /**
     * The log record batch format that writers may use. Level 2 allows the formats before V3, and
     * level 3 also allows {@link LogRecordBatchFormat#LOG_MAGIC_VALUE_V3}. The finalized level is
     * {@link ConfigOptions#CLUSTER_FEATURE_LOG_RECORD_BATCH_VERSION}.
     */
    public static final String LOG_RECORD_BATCH_VERSION = "log.record-batch.version";

    /** The level a tablet server is assumed to support when its registration has no features. */
    public static final int LOG_RECORD_BATCH_VERSION_BASELINE = 2;

    /** The highest level of {@link #LOG_RECORD_BATCH_VERSION} this server supports. */
    public static final int LOG_RECORD_BATCH_VERSION_MAX = LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;

    private ClusterFeatures() {}

    /** The features this server supports, mapped to the highest supported level. */
    public static Map<String, Integer> supportedFeatures() {
        return Collections.singletonMap(LOG_RECORD_BATCH_VERSION, LOG_RECORD_BATCH_VERSION_MAX);
    }

    /** Returns the highest level of the feature in the given supported features. */
    public static int supportedLevel(Map<String, Integer> supportedFeatures, String feature) {
        Integer level = supportedFeatures.get(feature);
        if (level != null) {
            return level;
        }
        return LOG_RECORD_BATCH_VERSION.equals(feature) ? LOG_RECORD_BATCH_VERSION_BASELINE : 0;
    }
}
