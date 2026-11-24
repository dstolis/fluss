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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.rpc.protocol.ApiKeys;

import java.util.EnumMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.ToIntFunction;

import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Declares, for each API that carries data of a {@link ClusterFeature}, the highest level of that
 * feature each API version can carry.
 *
 * <ul>
 *   <li>{@link Access#READ}: the API returns data to the reader. Every version is advertised. The
 *       version of a read request tells the server which data it may return as stored; data above
 *       that level must be converted before it is returned.
 *   <li>{@link Access#WRITE}: the API sends data to the server. A tablet server advertises a
 *       version only once it has applied the level that version carries, see {@link
 *       #highestAdvertisedVersion}, so a writer that negotiated the version knows the cluster
 *       finalized the level. The server checks both the version of the request and the level it has
 *       applied, see {@link #checkWrite(ApiKeys, short, ClusterFeature, int,
 *       ClusterFeatureLevels)}.
 * </ul>
 *
 * <p>Writers also learn the finalized levels from the {@code ApiVersions} response, see {@link
 * ClusterFeatureLevels#reportIn}. Both signals come from the same applied levels.
 *
 * <p>Adding a feature level to an API means adding a new API version to {@link ApiKeys} and
 * declaring here which level the new version carries.
 */
@Internal
public final class ApiFeatureContracts {

    private static final Map<ApiKeys, Contract> CONTRACTS = new EnumMap<>(ApiKeys.class);

    static {
        // Version 1 can read log record batches of magic V3.
        define(ApiKeys.FETCH_LOG, LOG_RECORD_BATCH_VERSION, Access.READ)
                .since((short) 0, LOG_RECORD_BATCH_VERSION.baselineLevel())
                .since((short) 1, 3);
        // Version 1 can read log record batches of magic V3 returned for log tables.
        define(ApiKeys.LIMIT_SCAN, LOG_RECORD_BATCH_VERSION, Access.READ)
                .since((short) 0, LOG_RECORD_BATCH_VERSION.baselineLevel())
                .since((short) 1, 3);
        // Version 2 may carry log record batches of magic V3.
        define(ApiKeys.PRODUCE_LOG, LOG_RECORD_BATCH_VERSION, Access.WRITE)
                .since((short) 0, LOG_RECORD_BATCH_VERSION.baselineLevel())
                .since((short) 2, 3);
        CONTRACTS.values().forEach(Contract::validate);
    }

    private ApiFeatureContracts() {}

    /** Whether the API returns data to the reader or sends data to the server. */
    public enum Access {
        READ,
        WRITE
    }

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
     * The highest version of the API a tablet server advertises, given the levels it has applied.
     * For a {@link Access#WRITE} API, versions that carry a level above the applied one are left
     * out, so a writer only negotiates a version for data the whole cluster can read.
     */
    public static short highestAdvertisedVersion(ApiKeys api, ClusterFeatureLevels applied) {
        Contract contract = CONTRACTS.get(api);
        if (contract == null || contract.access != Access.WRITE) {
            return api.highestSupportedVersion;
        }
        // the levels grow with the versions, so the first version above the applied level and all
        // versions after it are left out
        int appliedLevel = applied.level(contract.feature);
        for (Map.Entry<Short, Integer> entry : contract.levelsSince.entrySet()) {
            if (entry.getValue() > appliedLevel) {
                return (short) (entry.getKey() - 1);
            }
        }
        return api.highestSupportedVersion;
    }

    /**
     * The level of the given feature a server has finalized, as implied by the versions it
     * advertised for the {@link Access#WRITE} APIs. A server advertises a version only once it has
     * applied the level the version carries.
     *
     * @param negotiatedVersion the negotiated version of an API, or a negative number if the API
     *     was not negotiated
     */
    public static int levelImpliedByWriteVersions(
            ClusterFeature feature, ToIntFunction<ApiKeys> negotiatedVersion) {
        int level = feature.baselineLevel();
        for (Contract contract : CONTRACTS.values()) {
            if (contract.access != Access.WRITE || contract.feature != feature) {
                continue;
            }
            int version = negotiatedVersion.applyAsInt(contract.api);
            if (version >= 0) {
                level =
                        Math.max(
                                level, contract.levelsSince.floorEntry((short) version).getValue());
            }
        }
        return level;
    }

    /**
     * Checks that data of the given level may be written with the given version of a {@link
     * Access#WRITE} API, on a server that has applied the given levels.
     *
     * @throws UnsupportedVersionException if the version cannot carry the level, or the level is
     *     not finalized
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
                            "%s version %s carries cluster feature '%s' up to level %s, but the "
                                    + "request contains data of level %s. Ask the server for its "
                                    + "API versions again to negotiate a newer version.",
                            api.name(),
                            apiVersion,
                            feature.featureName(),
                            versionLevel,
                            dataLevel));
        }
        checkWrite(feature, dataLevel, finalized);
    }

    /**
     * Checks that data of the given level may be written, whatever API it came through, for example
     * the Kafka protocol adapter.
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

    private static Contract define(ApiKeys api, ClusterFeature feature, Access access) {
        Contract contract = new Contract(api, feature, access);
        checkState(CONTRACTS.put(api, contract) == null, "Duplicate contract for %s.", api);
        return contract;
    }

    /** The levels of a feature that readers of the versions of one API can decode. */
    @VisibleForTesting
    static final class Contract {
        private final ApiKeys api;
        private final ClusterFeature feature;
        private final Access access;
        // API version -> highest level the API carries from that version on
        private final NavigableMap<Short, Integer> levelsSince = new TreeMap<>();

        @VisibleForTesting
        Contract(ApiKeys api, ClusterFeature feature, Access access) {
            this.api = api;
            this.feature = feature;
            this.access = access;
        }

        @VisibleForTesting
        Contract since(short apiVersion, int level) {
            levelsSince.put(apiVersion, level);
            return this;
        }

        @VisibleForTesting
        void validate() {
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
            // Replicas copy data as stored, so they need a version that reads every level, and so
            // do readers that should get data of the latest level without a conversion. A release
            // can't add a level without a read version for it.
            checkState(
                    access != Access.READ || previous == feature.latestLevel(),
                    "%s reads %s up to level %s, but the latest level is %s. Add a %s version "
                            + "that reads the latest level.",
                    api,
                    feature.featureName(),
                    previous,
                    feature.latestLevel(),
                    api);
        }
    }
}
