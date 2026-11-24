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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentDir;
import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentFile;

/**
 * Copies of remote log segments for clients that cannot decode newer batch formats.
 *
 * <p>A client whose {@code FETCH_LOG} version reads batches only up to magic V2 cannot download a
 * remote segment that holds V3 batches. The leader then reads the segment on the client's behalf,
 * which costs a remote read for every fetch. So that repeated scans don't pay this, the leader
 * writes a copy of the segment once, in the background, with every batch above V2 rewritten as V0.
 * From then on it hands the copy out like any other remote segment, and the client downloads it.
 *
 * <p>A copy lives inside the directory of its segment, at {@code
 * {segment_dir}/{copy_id}/{start_offset}.log}, where the copy id is derived from the segment id.
 * Clients build a download path from a tablet dir and a segment id, so the copy is handed out with
 * the segment dir as its tablet dir and the copy id as its segment id. It is deleted together with
 * its segment, see {@link DefaultRemoteLogStorage#deleteLogSegmentFiles}.
 *
 * <p>A copy is first written to a temporary file and then renamed, so a copy that exists is
 * complete. Making a copy only costs storage when a client that can't read V3 reaches a segment
 * that holds V3 batches.
 */
@Internal
@ThreadSafe
public class RemoteLogCompatCopies implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteLogCompatCopies.class);

    /** Copies are made for clients that read batches up to this magic, i.e. FETCH_LOG v0. */
    public static final byte COPY_READER_MAX_MAGIC = LOG_MAGIC_VALUE_V2;

    private static final String TEMPORARY_SUFFIX = ".tmp";
    private static final int MAX_KNOWN_COPIES = 10_000;
    private static final long RETRY_AFTER_FAILURE_MS = 60_000L;

    /**
     * How long a copy is handed out before it is checked again in the background, so a copy that
     * was removed, for example by a cleanup of remote storage, is written again.
     */
    @VisibleForTesting static final long RECHECK_AFTER_MS = 10 * 60_000L;

    private final ExecutorService executor;
    private final Clock clock;
    private final Object lock = new Object();

    /** The copies known to exist, by segment id. */
    @GuardedBy("lock")
    private final Map<UUID, KnownCopy> knownCopies =
            new LinkedHashMap<UUID, KnownCopy>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<UUID, KnownCopy> eldest) {
                    return size() > MAX_KNOWN_COPIES;
                }
            };

    /** The segments whose copy is being looked up or written. */
    private final Set<UUID> pending = ConcurrentHashMap.newKeySet();

    /** When making the copy of a segment last failed, by segment id. */
    private final Map<UUID, Long> failedAt = new ConcurrentHashMap<>();

    public RemoteLogCompatCopies() {
        this(
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("fluss-remote-log-compat-copy")),
                SystemClock.getInstance());
    }

    @VisibleForTesting
    public RemoteLogCompatCopies(ExecutorService executor, Clock clock) {
        this.executor = executor;
        this.clock = clock;
    }

    /** The segment id of the copy of a segment. */
    public static UUID copyId(UUID segmentId) {
        return FlussPaths.remoteLogSegmentCopyId(segmentId);
    }

    /** The file of the copy of a segment whose files are in {@code segmentDir}. */
    public static FsPath copyFile(FsPath segmentDir, RemoteLogSegment segment) {
        return remoteLogSegmentFile(
                remoteLogSegmentDir(segmentDir, copyId(segment.remoteLogSegmentId())),
                segment.remoteLogStartOffset());
    }

    /** The temporary file the copy is written to before it is renamed to {@link #copyFile}. */
    public static FsPath temporaryCopyFile(FsPath segmentDir, RemoteLogSegment segment) {
        FsPath copyFile = copyFile(segmentDir, segment);
        return new FsPath(copyFile.getParent(), copyFile.getName() + TEMPORARY_SUFFIX);
    }

    /**
     * Returns the fetch info that hands out the copy of {@code segment}, or null if there is no
     * copy yet. Without a copy, it starts looking for one, or writing it, in the background. It
     * never touches remote storage on the calling thread.
     *
     * @param remoteLogDir the remote log dir of the segment's table
     * @param partitionName the partition of the segment's bucket, null if not partitioned
     * @param segment the segment the client reached
     * @param storage reads the data of the segment
     */
    @Nullable
    public RemoteLogFetchInfo copyFetchInfo(
            FsPath remoteLogDir,
            @Nullable String partitionName,
            RemoteLogSegment segment,
            RemoteLogStorage storage) {
        UUID segmentId = segment.remoteLogSegmentId();
        FsPath segmentDir = remoteLogSegmentDir(remoteLogDir, segment);
        KnownCopy copy;
        synchronized (lock) {
            copy = knownCopies.get(segmentId);
        }
        if (copy != null) {
            if (clock.milliseconds() - copy.checkedAtMs >= RECHECK_AFTER_MS) {
                // hand the copy out meanwhile, the check writes it again if it is gone
                startFindOrWriteCopy(segmentDir, remoteLogDir, segment, storage);
            }
            return new RemoteLogFetchInfo(
                    segmentDir.toString(),
                    partitionName,
                    Collections.singletonList(copySegment(segment, copy.size)),
                    // the client skips the batches before its fetch offset
                    0);
        }
        Long lastFailure = failedAt.get(segmentId);
        if (lastFailure != null && clock.milliseconds() - lastFailure < RETRY_AFTER_FAILURE_MS) {
            return null;
        }
        startFindOrWriteCopy(segmentDir, remoteLogDir, segment, storage);
        return null;
    }

    private void startFindOrWriteCopy(
            FsPath segmentDir,
            FsPath remoteLogDir,
            RemoteLogSegment segment,
            RemoteLogStorage storage) {
        UUID segmentId = segment.remoteLogSegmentId();
        if (pending.add(segmentId)) {
            try {
                executor.execute(() -> findOrWriteCopy(segmentDir, remoteLogDir, segment, storage));
            } catch (RejectedExecutionException e) {
                // shutting down
                pending.remove(segmentId);
            }
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    private void findOrWriteCopy(
            FsPath segmentDir,
            FsPath remoteLogDir,
            RemoteLogSegment segment,
            RemoteLogStorage storage) {
        UUID segmentId = segment.remoteLogSegmentId();
        FsPath copyFile = copyFile(segmentDir, segment);
        try {
            FileSystem fs = copyFile.getFileSystem();
            long size;
            if (fs.exists(copyFile)) {
                size = fs.getFileStatus(copyFile).getLen();
            } else {
                size = writeCopy(fs, segmentDir, remoteLogDir, segment, storage);
                LOG.info(
                        "Wrote a copy of remote log segment {} of {} with batches up to magic {} "
                                + "for clients that can't read newer batches, {} bytes.",
                        segmentId,
                        segment.tableBucket(),
                        COPY_READER_MAX_MAGIC,
                        size);
            }
            synchronized (lock) {
                knownCopies.put(segmentId, new KnownCopy((int) size, clock.milliseconds()));
            }
            failedAt.remove(segmentId);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to make a copy of remote log segment {} of {} for clients that can't "
                            + "read its batches. They keep reading it through the server.",
                    segmentId,
                    segment.tableBucket(),
                    e);
            failedAt.put(segmentId, clock.milliseconds());
        } finally {
            pending.remove(segmentId);
        }
    }

    private static long writeCopy(
            FileSystem fs,
            FsPath segmentDir,
            FsPath remoteLogDir,
            RemoteLogSegment segment,
            RemoteLogStorage storage)
            throws Exception {
        FsPath copyFile = copyFile(segmentDir, segment);
        FsPath temporaryFile = temporaryCopyFile(segmentDir, segment);
        fs.mkdirs(copyFile.getParent());
        long size;
        try (InputStream in = storage.fetchLogData(remoteLogDir, segment);
                FSDataOutputStream out = fs.create(temporaryFile, FileSystem.WriteMode.OVERWRITE)) {
            size =
                    RemoteLogSegmentReadUtils.copyConvertedBatches(
                            in, segment.segmentSizeInBytes(), out, COPY_READER_MAX_MAGIC);
        }
        if (!fs.rename(temporaryFile, copyFile)) {
            throw new IOException("Failed to rename " + temporaryFile + " to " + copyFile + ".");
        }
        return size;
    }

    private static RemoteLogSegment copySegment(RemoteLogSegment segment, int copySize) {
        return RemoteLogSegment.Builder.builder()
                .remoteLogSegmentId(copyId(segment.remoteLogSegmentId()))
                .remoteLogStartOffset(segment.remoteLogStartOffset())
                .remoteLogEndOffset(segment.remoteLogEndOffset())
                .logicalStartOffset(segment.logicalStartOffset())
                .logicalEndOffset(segment.logicalEndOffset())
                .maxTimestamp(segment.maxTimestamp())
                .segmentSizeInBytes(copySize)
                .physicalTablePath(segment.physicalTablePath())
                .tableBucket(segment.tableBucket())
                .maxBatchMagic(COPY_READER_MAX_MAGIC)
                .build();
    }

    /** A copy known to exist, and when that was last checked. */
    private static final class KnownCopy {
        private final int size;
        private final long checkedAtMs;

        private KnownCopy(int size, long checkedAtMs) {
            this.size = size;
            this.checkedAtMs = checkedAtMs;
        }
    }
}
