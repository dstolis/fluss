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

package org.apache.fluss.server.replica;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.BytesViewLogRecords;
import org.apache.fluss.record.FileLogProjection;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordBatchConverter;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.remote.RemoteLogCompatCopies;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.log.remote.RemoteLogSegmentReadUtils;
import org.apache.fluss.utils.FlussPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Serves remote log reads to clients that cannot decode every batch format stored in remote
 * segments, such as a client whose {@code FETCH_LOG} version reads batches only up to magic V2.
 *
 * <p>The leading segments the client can decode are handed out as usual. For the first segment it
 * can't decode, the client gets the segment's V0 copy if there is one, see {@link
 * RemoteLogCompatCopies}. Until the copy is written, the server reads the segment on the client's
 * behalf and returns the converted batches inline.
 */
class OldReaderRemoteFetcher implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(OldReaderRemoteFetcher.class);

    private final RemoteLogManager remoteLogManager;
    private final RemoteLogCompatCopies compatCopies;

    OldReaderRemoteFetcher(RemoteLogManager remoteLogManager, RemoteLogCompatCopies compatCopies) {
        this.remoteLogManager = remoteLogManager;
        this.compatCopies = compatCopies;
    }

    /**
     * Prepares a remote fetch result for a client that cannot read every batch format.
     *
     * <p>When the batches are read inline, at most {@code maxBytes} are read from the fetch
     * position, but at least one complete batch. The client then continues fetching from the next
     * offset as usual.
     */
    FetchLogResultForBucket fetch(
            Replica replica,
            FetchLogResultForBucket result,
            int maxBytes,
            FetchParams fetchParams) {
        RemoteLogFetchInfo remoteInfo = result.remoteLogFetchInfo();
        if (remoteInfo == null || remoteInfo.remoteLogSegmentList().isEmpty()) {
            return result;
        }
        TableBucket tb = replica.getTableBucket();
        byte readerMaxMagic = fetchParams.readerMaxBatchMagic();
        List<RemoteLogSegment> readable =
                readableSegments(remoteInfo.remoteLogSegmentList(), readerMaxMagic);
        if (!readable.isEmpty()) {
            // the reader can decode these segments itself, so hand them out as usual
            LOG.debug(
                    "Handing out {} remote log segments of {} as stored to a reader of batch magic "
                            + "up to {}.",
                    readable.size(),
                    tb,
                    readerMaxMagic);
            return FetchLogResultForBucket.remote(
                    tb,
                    new RemoteLogFetchInfo(
                            remoteInfo.remoteLogTabletDir(),
                            remoteInfo.partitionName(),
                            readable,
                            remoteInfo.firstStartPos()),
                    result.getHighWatermark());
        }
        RemoteLogSegment segment = remoteInfo.remoteLogSegmentList().get(0);
        FsPath remoteLogDir = FlussPaths.remoteLogDir(replica.getRemoteDataDir());
        // every reader reads the copy: the contracts start every read API at the baseline level,
        // which is the batch magic of the copy
        RemoteLogFetchInfo copy =
                compatCopies.copyFetchInfo(
                        remoteLogDir,
                        remoteInfo.partitionName(),
                        segment,
                        remoteLogManager.getRemoteLogStorage());
        if (copy != null) {
            LOG.debug(
                    "Handing out the copy of remote log segment {} of {} to a reader of batch "
                            + "magic up to {}.",
                    segment.remoteLogSegmentId(),
                    tb,
                    readerMaxMagic);
            return FetchLogResultForBucket.remote(tb, copy, result.getHighWatermark());
        }
        LOG.debug(
                "Converting remote log segment {} of {} with batch magic up to {} inline for a "
                        + "reader of batch magic up to {}.",
                segment.remoteLogSegmentId(),
                tb,
                segment.maxBatchMagic(),
                readerMaxMagic);
        try (InputStream in =
                remoteLogManager.getRemoteLogStorage().fetchLogData(remoteLogDir, segment)) {
            long fetchOffset =
                    fetchParams.fetchOffset() == FetchParams.FETCH_FROM_EARLIEST_OFFSET
                            ? replica.getLogStartOffset()
                            : fetchParams.fetchOffset();
            byte[] bytes =
                    RemoteLogSegmentReadUtils.readBatches(
                            in,
                            remoteInfo.firstStartPos(),
                            segment.segmentSizeInBytes(),
                            fetchOffset,
                            Math.max(1, maxBytes));
            LogRecords records = projectIfNeeded(bytes, fetchParams.projection(), maxBytes);
            return FetchLogResultForBucket.records(
                    tb,
                    LogRecordBatchConverter.convert(records, readerMaxMagic),
                    result.getHighWatermark(),
                    -1L,
                    -1L);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to read remote log segment {} of {} for an old reader.",
                    segment,
                    tb,
                    e);
            return FetchLogResultForBucket.error(tb, ApiError.fromThrowable(e));
        }
    }

    @Override
    public void close() {
        compatCopies.close();
    }

    /**
     * The leading segments whose batches the reader can decode. Segments uploaded by versions that
     * did not record their batch magic predate V3, so they only hold formats every reader knows.
     */
    private static List<RemoteLogSegment> readableSegments(
            List<RemoteLogSegment> segments, byte readerMaxBatchMagic) {
        List<RemoteLogSegment> readable = new ArrayList<>();
        for (RemoteLogSegment segment : segments) {
            if (segment.maxBatchMagic() > readerMaxBatchMagic) {
                break;
            }
            readable.add(segment);
        }
        return readable;
    }

    /** Applies the fetch's column projection, which needs the batches in a file. */
    private static LogRecords projectIfNeeded(
            byte[] bytes, @Nullable FileLogProjection projection, int maxBytes) throws Exception {
        if (projection == null || bytes.length == 0) {
            return MemoryLogRecords.pointToBytes(bytes);
        }
        File file = File.createTempFile("fluss-remote-fetch", ".log");
        try {
            Files.write(file.toPath(), bytes);
            try (FileLogRecords fileRecords = FileLogRecords.open(file)) {
                BytesViewLogRecords projected =
                        projection.project(
                                fileRecords.channel(), 0, bytes.length, Math.max(1, maxBytes));
                // copy the projected batches before the file is closed and deleted
                return LogRecordBatchConverter.copyToMemory(projected);
            }
        } finally {
            Files.deleteIfExists(file.toPath());
        }
    }
}
