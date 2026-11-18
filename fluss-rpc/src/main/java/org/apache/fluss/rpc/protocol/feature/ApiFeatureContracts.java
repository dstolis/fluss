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
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.rpc.protocol.ApiKeys;

import java.util.EnumMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Declares, for each API that returns data of a {@link ClusterFeature} to a reader, the highest
 * level of that feature a reader of each API version can decode. The version of a read request
 * therefore tells the server which data it may return as stored. Data above that level must be
 * converted before it is returned.
 *
 * <p>APIs that send data to the server need no declaration. A writer learns the finalized levels
 * from the {@code ApiVersions} response of the tablet servers, see {@link
 * ClusterFeatureLevels#reportIn}, and the server checks the data it receives against the level it
 * has applied, see {@link #checkWrite}.
 *
 * <p>Adding a feature level to a read API means adding a new API version to {@link ApiKeys} and
 * declaring here which level a reader of the new version can decode.
 */
@Internal
public final class ApiFeatureContracts {

    private static final Map<ApiKeys, Contract> CONTRACTS = new EnumMap<>(ApiKeys.class);

    static {
        // Version 1 can read log record batches of magic V3.
        define(ApiKeys.FETCH_LOG, LOG_RECORD_BATCH_VERSION)
                .since((short) 0, LOG_RECORD_BATCH_VERSION.baselineLevel())
                .since((short) 1, 3);
        // Version 1 can read log record batches of magic V3 returned for log tables.
        define(ApiKeys.LIMIT_SCAN, LOG_RECORD_BATCH_VERSION)
                .since((short) 0, LOG_RECORD_BATCH_VERSION.baselineLevel())
                .since((short) 1, 3);
        CONTRACTS.values().forEach(Contract::validate);
    }

    private ApiFeatureContracts() {}

    /**
     * The highest level of the given feature that a reader of the given API version can decode.
     * APIs without a contract for the feature return no data of it, so any level is fine for them.
     */
    public static int maxLevel(ApiKeys api, short apiVersion, ClusterFeature feature) {
        Contract contract = CONTRACTS.get(api);
        if (contract == null || contract.feature != feature) {
            return Integer.MAX_VALUE;
        }
        return contract.levelsSince.floorEntry(apiVersion).getValue();
    }

    /**
     * Checks that data of the given level may be written, whatever the API version of the request.
     *
     * @throws UnsupportedVersionException if the cluster has not finalized the level yet
     */
    public static void checkWrite(
            ClusterFeature feature, int dataLevel, ClusterFeatureLevels finalized) {
        int finalizedLevel = finalized.level(feature);
        if (dataLevel > finalizedLevel) {
            throw new UnsupportedVersionException(
                    String.format(
                            "The request contains data of level %s of cluster feature '%s', but "
                                    + "the cluster has finalized level %s.",
                            dataLevel, feature.featureName(), finalizedLevel));
        }
    }

    private static Contract define(ApiKeys api, ClusterFeature feature) {
        Contract contract = new Contract(api, feature);
        checkState(CONTRACTS.put(api, contract) == null, "Duplicate contract for %s.", api);
        return contract;
    }

    /** The levels of a feature that readers of the versions of one API can decode. */
    private static final class Contract {
        private final ApiKeys api;
        private final ClusterFeature feature;
        // API version -> highest level a reader can decode from that version on
        private final NavigableMap<Short, Integer> levelsSince = new TreeMap<>();

        private Contract(ApiKeys api, ClusterFeature feature) {
            this.api = api;
            this.feature = feature;
        }

        private Contract since(short apiVersion, int level) {
            levelsSince.put(apiVersion, level);
            return this;
        }

        private void validate() {
            checkState(
                    levelsSince.containsKey(api.lowestSupportedVersion),
                    "The contract of %s must start at version %s.",
                    api,
                    api.lowestSupportedVersion);
            checkState(
                    levelsSince.firstEntry().getValue() == feature.baselineLevel(),
                    "The lowest version of %s must carry the baseline level of %s.",
                    api,
                    feature.featureName());
            int previous = Integer.MIN_VALUE;
            for (Map.Entry<Short, Integer> entry : levelsSince.entrySet()) {
                checkArgument(
                        entry.getKey() <= api.highestSupportedVersion,
                        "%s has no version %s.",
                        api,
                        entry.getKey());
                checkState(
                        entry.getValue() > previous && entry.getValue() <= feature.latestLevel(),
                        "The levels of %s must grow with its versions, up to level %s.",
                        api,
                        feature.latestLevel());
                previous = entry.getValue();
            }
        }
    }
}
