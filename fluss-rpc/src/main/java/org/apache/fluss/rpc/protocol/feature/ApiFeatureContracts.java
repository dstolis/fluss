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
import org.apache.fluss.rpc.messages.PbApiVersion;
import org.apache.fluss.rpc.protocol.ApiKeys;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Declares, for each API whose data depends on a {@link ClusterFeature}, the highest level of that
 * feature each API version can carry. The RPC version then decides which formats a client and a
 * server may exchange, and the data itself, for example the magic of a log record batch, decides
 * how it is decoded.
 *
 * <p>A contract is either:
 *
 * <ul>
 *   <li>{@link Access#WRITE}: the API sends data to the server. A version that can carry a level
 *       above the finalized level is not advertised, so clients only write data the whole cluster
 *       can read, and the server rejects data above the finalized level or above the version's
 *       level.
 *   <li>{@link Access#READ}: the API returns data to the reader. Every version is advertised, and
 *       data above the level of the reader's version must not be returned as stored.
 * </ul>
 *
 * <p>Adding a feature level to an API means adding a new API version to {@link ApiKeys} and
 * declaring here which level the new version can carry.
 */
@Internal
public final class ApiFeatureContracts {

    /** Whether an API sends data of a feature to the server or returns it to the reader. */
    public enum Access {
        READ,
        WRITE
    }

    private static final Map<ApiKeys, Contract> CONTRACTS = new EnumMap<>(ApiKeys.class);

    static {
        // Version 2 may carry log record batches of magic V3.
        define(ApiKeys.PRODUCE_LOG, LOG_RECORD_BATCH_VERSION, Access.WRITE)
                .since((short) 0, LOG_RECORD_BATCH_VERSION.baselineLevel())
                .since((short) 2, 3);
        // Version 1 can read log record batches of magic V3.
        define(ApiKeys.FETCH_LOG, LOG_RECORD_BATCH_VERSION, Access.READ)
                .since((short) 0, LOG_RECORD_BATCH_VERSION.baselineLevel())
                .since((short) 1, 3);
        // Version 1 can read log record batches of magic V3 returned for log tables.
        define(ApiKeys.LIMIT_SCAN, LOG_RECORD_BATCH_VERSION, Access.READ)
                .since((short) 0, LOG_RECORD_BATCH_VERSION.baselineLevel())
                .since((short) 1, 3);
        CONTRACTS.values().forEach(Contract::validate);
    }

    private ApiFeatureContracts() {}

    /**
     * The highest level of the given feature that the given API version can carry. APIs without a
     * contract for the feature carry no data of it, so any level is fine for them.
     */
    public static int maxLevel(ApiKeys api, short apiVersion, ClusterFeature feature) {
        Contract contract = CONTRACTS.get(api);
        if (contract == null || contract.feature != feature) {
            return Integer.MAX_VALUE;
        }
        return contract.levelsSince.floorEntry(apiVersion).getValue();
    }

    /**
     * The highest version of the given API a server advertises with the given finalized levels. For
     * a {@link Access#WRITE} contract it is the highest version whose level is finalized, for every
     * other API it is the highest version the server supports.
     */
    public static short highestEnabledVersion(ApiKeys api, ClusterFeatureLevels finalized) {
        Contract contract = CONTRACTS.get(api);
        short highest = api.highestSupportedVersion;
        if (contract == null || contract.access != Access.WRITE) {
            return highest;
        }
        int finalizedLevel = finalized.level(contract.feature);
        while (highest > api.lowestSupportedVersion
                && contract.levelsSince.floorEntry(highest).getValue() > finalizedLevel) {
            highest--;
        }
        return highest;
    }

    /** Lowers the advertised max version of every API to what the finalized levels enable. */
    public static void capAdvertisedVersions(
            List<PbApiVersion> apiVersions, ClusterFeatureLevels finalized) {
        for (PbApiVersion apiVersion : apiVersions) {
            if (!ApiKeys.hasId(apiVersion.getApiKey())) {
                continue;
            }
            short enabled = highestEnabledVersion(ApiKeys.forId(apiVersion.getApiKey()), finalized);
            if (apiVersion.getMaxVersion() > enabled) {
                apiVersion.setMaxVersion(enabled);
            }
        }
    }

    /**
     * Checks that data of the given level may be written with the given API version.
     *
     * @throws UnsupportedVersionException if the API version cannot carry the level, or the cluster
     *     has not finalized it yet
     */
    public static void checkWrite(
            ApiKeys api,
            short apiVersion,
            ClusterFeature feature,
            int dataLevel,
            ClusterFeatureLevels finalized) {
        int versionLevel = maxLevel(api, apiVersion, feature);
        if (dataLevel > versionLevel) {
            throw new UnsupportedVersionException(
                    String.format(
                            "%s version %s carries cluster feature '%s' up to level %s, "
                                    + "but the request contains data of level %s.",
                            api.name(),
                            apiVersion,
                            feature.featureName(),
                            versionLevel,
                            dataLevel));
        }
        int finalizedLevel = finalized.level(feature);
        if (dataLevel > finalizedLevel) {
            throw new UnsupportedVersionException(
                    String.format(
                            "The request contains data of level %s of cluster feature '%s', but "
                                    + "the cluster has finalized level %s, see '%s'.",
                            dataLevel,
                            feature.featureName(),
                            finalizedLevel,
                            feature.configOption().key()));
        }
    }

    /**
     * Whether data of the given level can be returned as stored to a reader of the given version.
     */
    public static boolean canRead(
            ApiKeys api, short apiVersion, ClusterFeature feature, int dataLevel) {
        return dataLevel <= maxLevel(api, apiVersion, feature);
    }

    private static Contract define(ApiKeys api, ClusterFeature feature, Access access) {
        Contract contract = new Contract(api, feature, access);
        checkState(CONTRACTS.put(api, contract) == null, "Duplicate contract for %s.", api);
        return contract;
    }

    /** The levels of a feature that the versions of one API can carry. */
    private static final class Contract {
        private final ApiKeys api;
        private final ClusterFeature feature;
        private final Access access;
        // API version -> highest level carried from that version on
        private final NavigableMap<Short, Integer> levelsSince = new TreeMap<>();

        private Contract(ApiKeys api, ClusterFeature feature, Access access) {
            this.api = api;
            this.feature = feature;
            this.access = access;
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
