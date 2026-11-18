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
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.feature.ApiFeatureContracts;
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;

/**
 * The finalized level of every cluster feature, as applied on this tablet server. It follows the
 * levels the coordinator stores for the cluster, which it only raises once every server supports
 * the new level. Every feature is at its baseline until the stored levels are applied.
 *
 * <p>The applied levels follow the stored ones through a watch, so they can lag behind for a short
 * time after a raise. Before this server rejects something because a level is too low, it reads the
 * stored levels again, as Kafka brokers do, so a writer that learned the new level from another
 * server is not turned away. The read is made at most once per {@link #REFRESH_INTERVAL_MS}.
 */
@Internal
@ThreadSafe
public class FinalizedClusterFeatures {

    private static final Logger LOG = LoggerFactory.getLogger(FinalizedClusterFeatures.class);

    /** The minimum time between two reads of the stored levels before a rejection. */
    public static final long REFRESH_INTERVAL_MS = 1000L;

    private final Clock clock;

    /** When the stored levels were last read before a rejection, in clock milliseconds. */
    private final AtomicLong lastRefreshMs = new AtomicLong(Long.MIN_VALUE);

    private volatile ClusterFeatureLevels levels = ClusterFeatureLevels.baseline();

    /** Reads the stored levels, absent if the cluster has none yet; null until set. */
    @Nullable
    private volatile SupplierWithException<Optional<Map<String, Integer>>, Exception> storedLevels;

    public FinalizedClusterFeatures() {
        this(SystemClock.getInstance());
    }

    public FinalizedClusterFeatures(Clock clock) {
        this.clock = clock;
    }

    /**
     * Sets how the stored levels are read again before something is rejected for a too low level.
     * Without it, only the levels given to {@link #apply} count.
     */
    public void setStoredLevelsReader(
            SupplierWithException<Optional<Map<String, Integer>>, Exception> storedLevels) {
        this.storedLevels = storedLevels;
    }

    /** The levels applied on this server. */
    public ClusterFeatureLevels levels() {
        return levels;
    }

    /**
     * Whether the given feature is finalized at the given level or higher. If the applied level is
     * lower, the stored levels are read again first, at most once per {@link #REFRESH_INTERVAL_MS}.
     */
    public boolean isFinalizedAtLeast(ClusterFeature feature, int level) {
        if (levels.level(feature) >= level) {
            return true;
        }
        refresh();
        return levels.level(feature) >= level;
    }

    /**
     * Checks that every log record batch a writer sent has a format the cluster has finalized, so
     * no replica or reader receives batches it cannot read. Every leader append runs this check,
     * whichever API or protocol adapter the batches came through.
     *
     * <p>The level this tablet server has applied decides, not what the writer has learned: a
     * writer that learned a level from another tablet server also uses it with a leader that has
     * not applied the level yet. That leader rejects the batch, and the writer sends it again.
     *
     * @return the error to answer the batches with, or null if they can be appended
     */
    @Nullable
    public ApiError checkWrite(LogRecords records) {
        if (levels.level(LOG_RECORD_BATCH_VERSION) >= LOG_RECORD_BATCH_VERSION.latestLevel()) {
            // every format this version can store is finalized
            return null;
        }
        UnsupportedVersionException unsupported = findUnfinalizedBatch(records);
        if (unsupported == null) {
            return null;
        }
        // the level may have been raised without this server having applied it yet
        refresh();
        unsupported = findUnfinalizedBatch(records);
        return unsupported == null ? null : ApiError.fromThrowable(unsupported);
    }

    @Nullable
    private UnsupportedVersionException findUnfinalizedBatch(LogRecords records) {
        ClusterFeatureLevels applied = levels;
        try {
            for (LogRecordBatch batch : records.batches()) {
                ApiFeatureContracts.checkWrite(LOG_RECORD_BATCH_VERSION, batch.magic(), applied);
            }
            return null;
        } catch (UnsupportedVersionException e) {
            return e;
        }
    }

    /**
     * Reads the stored levels and applies them, unless they were read less than {@link
     * #REFRESH_INTERVAL_MS} ago. Only one caller reads at a time; the others go on with the applied
     * levels. A level this build does not support is left to the watch, which stops the server.
     */
    private void refresh() {
        SupplierWithException<Optional<Map<String, Integer>>, Exception> reader = storedLevels;
        if (reader == null) {
            return;
        }
        long now = clock.milliseconds();
        long last = lastRefreshMs.get();
        if ((last != Long.MIN_VALUE && now - last < REFRESH_INTERVAL_MS)
                || !lastRefreshMs.compareAndSet(last, now)) {
            return;
        }
        try {
            Optional<Map<String, Integer>> stored = reader.get();
            if (!stored.isPresent()) {
                return;
            }
            List<String> unsupported = ClusterFeatures.unsupportedByThisBuild(stored.get());
            if (!unsupported.isEmpty()) {
                LOG.warn(
                        "The stored cluster feature levels contain {}, which this tablet server "
                                + "does not support, so they are not applied.",
                        unsupported);
                return;
            }
            apply(ClusterFeatureLevels.fromMap(stored.get()));
        } catch (Exception e) {
            LOG.warn("Failed to read the stored cluster feature levels.", e);
        }
    }

    /**
     * Applies the finalized levels of the cluster. A level is never lowered: data written at the
     * applied level may already exist, so a lower level than the applied one is ignored.
     */
    public synchronized void apply(ClusterFeatureLevels finalized) {
        ClusterFeatureLevels raised = levels.max(finalized);
        if (!raised.equals(finalized)) {
            LOG.error(
                    "Ignoring finalized cluster feature levels {} that are below the applied "
                            + "levels {}. Levels must never be lowered.",
                    finalized,
                    levels);
        }
        if (!raised.equals(levels)) {
            LOG.info("Applying the finalized cluster feature levels {}.", raised);
            levels = raised;
        }
    }
}
