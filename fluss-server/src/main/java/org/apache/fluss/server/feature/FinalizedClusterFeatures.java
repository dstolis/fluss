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
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;

/**
 * The finalized level of every cluster feature, as applied on this tablet server. It follows the
 * levels the coordinator stores for the cluster, which it only raises once every server supports
 * the new level. Every feature is at its baseline until the stored levels are applied.
 *
 * <p>The applied levels follow the stored ones through a watch, so they can lag behind for a short
 * time after a raise. When this server finds a level too low, it also reads the stored levels again
 * in the background, so a writer that learned the new level from another server is turned away only
 * until the read is applied. Request threads never wait for the read: at most one read runs at a
 * time, and a new one starts at most once per {@link #REFRESH_INTERVAL_MS}.
 *
 * <p>Together with the levels, it keeps the feature epoch they were stored at, so the server can
 * report how far it has caught up with the raises.
 */
@Internal
@ThreadSafe
public class FinalizedClusterFeatures {

    private static final Logger LOG = LoggerFactory.getLogger(FinalizedClusterFeatures.class);

    /** The minimum time between two reads of the stored levels before a rejection. */
    public static final long REFRESH_INTERVAL_MS = 1000L;

    private final Clock clock;

    /** When a read of the stored levels last started, in clock milliseconds. */
    private final AtomicLong lastRefreshMs = new AtomicLong(Long.MIN_VALUE);

    /** Whether a read of the stored levels is running. */
    private final AtomicBoolean refreshing = new AtomicBoolean(false);

    /** The feature epoch of levels that were never stored. */
    public static final int NO_EPOCH = -1;

    private volatile ClusterFeatureLevels levels = ClusterFeatureLevels.baseline();

    private volatile int epoch = NO_EPOCH;

    /** Reads the stored levels and their epoch, absent if the cluster has none yet. */
    @Nullable
    private volatile SupplierWithException<Optional<VersionedData<Map<String, Integer>>>, Exception>
            storedLevels;

    /** Runs the reads of the stored levels, never on the thread that asks for one. */
    @Nullable private volatile Executor refreshExecutor;

    /** Called after the applied levels or their epoch changed. */
    @Nullable private volatile Runnable appliedListener;

    public FinalizedClusterFeatures() {
        this(SystemClock.getInstance());
    }

    public FinalizedClusterFeatures(Clock clock) {
        this.clock = clock;
    }

    /**
     * Sets how the stored levels are read again when a level is found too low, and the executor
     * that runs the reads. The reads block, for example on ZooKeeper, so the executor must not be a
     * request thread. Without a reader, only the levels given to {@link #apply} count.
     */
    public void setStoredLevelsReader(
            SupplierWithException<Optional<VersionedData<Map<String, Integer>>>, Exception>
                    storedLevels,
            Executor refreshExecutor) {
        this.refreshExecutor = refreshExecutor;
        this.storedLevels = storedLevels;
    }

    /**
     * Sets what to call after the applied levels or their epoch changed. It runs on the thread that
     * applied them, so it must not block.
     */
    public void setAppliedListener(Runnable appliedListener) {
        this.appliedListener = appliedListener;
    }

    /** The levels applied on this server. */
    public ClusterFeatureLevels levels() {
        return levels;
    }

    /** The feature epoch of the applied levels, {@link #NO_EPOCH} if none were stored yet. */
    public int epoch() {
        return epoch;
    }

    /**
     * Whether the given feature is finalized at the given level or higher, as applied. If the
     * applied level is lower, a read of the stored levels is started in the background, see {@link
     * #refresh}, so a later call sees a raise the watch has not applied yet.
     */
    public boolean isFinalizedAtLeast(ClusterFeature feature, int level) {
        if (levels.level(feature) >= level) {
            return true;
        }
        refresh();
        return false;
    }

    /**
     * Starts a read of the stored levels in the background, see {@link #refresh}, unless every
     * feature is applied at the latest level this build supports.
     */
    public void refreshIfBelowLatest() {
        if (!levels.isAtLeast(ClusterFeatureLevels.latest())) {
            refresh();
        }
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
        ClusterFeatureLevels applied = levels;
        try {
            for (LogRecordBatch batch : records.batches()) {
                ApiFeatureContracts.checkWrite(LOG_RECORD_BATCH_VERSION, batch.magic(), applied);
            }
            return null;
        } catch (UnsupportedVersionException e) {
            // the level may have been raised without this server having applied it yet: read it
            // again in the background, the writer retries the rejected batches
            refresh();
            return ApiError.fromThrowable(e);
        }
    }

    /**
     * Starts a read of the stored levels on the refresh executor, which applies them, unless a read
     * is running or one started less than {@link #REFRESH_INTERVAL_MS} ago. The caller goes on with
     * the applied levels. A level this build does not support is left to the watch, which stops the
     * server.
     */
    private void refresh() {
        SupplierWithException<Optional<VersionedData<Map<String, Integer>>>, Exception> reader =
                storedLevels;
        Executor executor = refreshExecutor;
        if (reader == null || executor == null) {
            return;
        }
        long now = clock.milliseconds();
        long last = lastRefreshMs.get();
        if (last != Long.MIN_VALUE && now - last < REFRESH_INTERVAL_MS) {
            return;
        }
        if (!refreshing.compareAndSet(false, true)) {
            return;
        }
        lastRefreshMs.set(now);
        try {
            executor.execute(() -> readAndApply(reader));
        } catch (Exception e) {
            refreshing.set(false);
            LOG.warn("Failed to start reading the stored cluster feature levels.", e);
        }
    }

    private void readAndApply(
            SupplierWithException<Optional<VersionedData<Map<String, Integer>>>, Exception>
                    reader) {
        try {
            Optional<VersionedData<Map<String, Integer>>> stored = reader.get();
            if (!stored.isPresent()) {
                return;
            }
            List<String> unsupported = applyStored(stored.get().data(), stored.get().zkVersion());
            if (!unsupported.isEmpty()) {
                LOG.warn(
                        "The stored cluster feature levels contain {}, which this tablet server "
                                + "does not support, so they are not applied.",
                        unsupported);
            }
        } catch (Exception e) {
            LOG.warn("Failed to read the stored cluster feature levels.", e);
        } finally {
            refreshing.set(false);
        }
    }

    /**
     * Applies the levels stored for the cluster at the given feature epoch, unless this build does
     * not support all of them, see {@link ClusterFeatures#unsupportedByThisBuild}.
     *
     * @return the stored levels this build does not support, empty if the levels were applied
     */
    public List<String> applyStored(Map<String, Integer> storedLevels, int storedEpoch) {
        List<String> unsupported = ClusterFeatures.unsupportedByThisBuild(storedLevels);
        if (unsupported.isEmpty()) {
            apply(ClusterFeatureLevels.fromMap(storedLevels), storedEpoch);
        }
        return unsupported;
    }

    /**
     * Applies the finalized levels of the cluster, stored at the given feature epoch. A level is
     * never lowered: data written at the applied level may already exist, so a lower level than the
     * applied one is ignored, and so is an older epoch.
     */
    public void apply(ClusterFeatureLevels finalized, int finalizedEpoch) {
        boolean changed;
        synchronized (this) {
            ClusterFeatureLevels before = levels;
            int epochBefore = epoch;
            applyLevels(finalized);
            epoch = Math.max(epoch, finalizedEpoch);
            changed = !before.equals(levels) || epochBefore != epoch;
        }
        Runnable listener = appliedListener;
        if (changed && listener != null) {
            listener.run();
        }
    }

    /**
     * Applies levels without a known feature epoch, as {@link #apply(ClusterFeatureLevels, int)}.
     */
    public void apply(ClusterFeatureLevels finalized) {
        apply(finalized, NO_EPOCH);
    }

    private void applyLevels(ClusterFeatureLevels finalized) {
        if (!finalized.isAtLeast(levels)) {
            LOG.error(
                    "Ignoring finalized cluster feature levels {} that are below the applied "
                            + "levels {}. Levels must never be lowered.",
                    finalized,
                    levels);
        }
        ClusterFeatureLevels raised = levels.max(finalized);
        if (!raised.equals(levels)) {
            LOG.info("Applying the finalized cluster feature levels {}.", raised);
            levels = raised;
        }
    }
}
