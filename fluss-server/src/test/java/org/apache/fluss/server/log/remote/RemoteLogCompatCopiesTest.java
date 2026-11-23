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

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentDir;
import static org.apache.fluss.utils.FlussPaths.remoteLogSegmentFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteLogCompatCopies}. */
class RemoteLogCompatCopiesTest extends RemoteLogTestBase {

    private ExecutorService ioExecutor;
    private DefaultRemoteLogStorage storage;
    private ManuallyTriggeredScheduledExecutorService copyExecutor;
    private ManualClock clock;
    private RemoteLogCompatCopies copies;
    private FsPath remoteLogDir;
    private RemoteLogSegment segment;
    private FsPath segmentDir;

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        ioExecutor = Executors.newSingleThreadExecutor();
        storage = new DefaultRemoteLogStorage(conf, ioExecutor);
        copyExecutor = new ManuallyTriggeredScheduledExecutorService();
        clock = new ManualClock(System.currentTimeMillis());
        copies = new RemoteLogCompatCopies(copyExecutor, clock);
        remoteLogDir = FlussPaths.remoteLogDir(conf);
        LogTablet logTablet = makeLogTabletAndAddSegments(false);
        segment = copyLogSegmentToRemote(logTablet, storage, 0);
        segmentDir = remoteLogSegmentDir(remoteLogDir, segment);
    }

    @AfterEach
    public void teardown() throws Exception {
        copies.close();
        storage.close();
        ioExecutor.shutdown();
    }

    @Test
    void testCopyIsWrittenInTheBackgroundAndThenHandedOut() throws Exception {
        // no copy yet: the caller reads the segment itself, and the copy is written in the
        // background
        assertThat(copies.copyFetchInfo(remoteLogDir, null, segment, storage)).isNull();
        assertThat(copyExecutor.numQueuedRunnables()).isEqualTo(1);
        // asking again while the copy is written doesn't start a second one
        assertThat(copies.copyFetchInfo(remoteLogDir, null, segment, storage)).isNull();
        assertThat(copyExecutor.numQueuedRunnables()).isEqualTo(1);
        copyExecutor.triggerAll();

        RemoteLogFetchInfo fetchInfo = copies.copyFetchInfo(remoteLogDir, null, segment, storage);
        assertThat(fetchInfo).isNotNull();
        // a client finds the copy at {tablet dir}/{segment id}/{start offset}.log
        assertThat(fetchInfo.remoteLogTabletDir()).isEqualTo(segmentDir.toString());
        assertThat(fetchInfo.firstStartPos()).isEqualTo(0);
        assertThat(fetchInfo.remoteLogSegmentList()).hasSize(1);
        RemoteLogSegment copySegment = fetchInfo.remoteLogSegmentList().get(0);
        assertThat(copySegment.remoteLogSegmentId())
                .isEqualTo(RemoteLogCompatCopies.copyId(segment.remoteLogSegmentId()));
        assertThat(copySegment.remoteLogStartOffset()).isEqualTo(segment.remoteLogStartOffset());
        assertThat(copySegment.remoteLogEndOffset()).isEqualTo(segment.remoteLogEndOffset());
        assertThat(copySegment.maxBatchMagic()).isEqualTo(LOG_MAGIC_VALUE_V2);

        File copyFile =
                localFile(
                        remoteLogSegmentFile(
                                remoteLogSegmentDir(
                                        new FsPath(fetchInfo.remoteLogTabletDir()),
                                        copySegment.remoteLogSegmentId()),
                                copySegment.remoteLogStartOffset()));
        assertThat(copySegment.segmentSizeInBytes()).isEqualTo((int) copyFile.length());
        // the batches of this segment are readable by every client, so they are copied as stored
        assertThat(baseOffsets(copyFile)).isEqualTo(baseOffsets(originalFile()));
        assertThat(localFile(RemoteLogCompatCopies.temporaryCopyFile(segmentDir, segment)))
                .doesNotExist();
        assertThat(copyExecutor.numQueuedRunnables()).isEqualTo(0);
    }

    @Test
    void testAnotherServerFindsTheExistingCopy() throws Exception {
        assertThat(copies.copyFetchInfo(remoteLogDir, null, segment, storage)).isNull();
        copyExecutor.triggerAll();

        // a new leader that never saw the copy finds it, without reading the segment again
        assertThat(originalFile().delete()).isTrue();
        RemoteLogCompatCopies otherServer = new RemoteLogCompatCopies(copyExecutor, clock);
        assertThat(otherServer.copyFetchInfo(remoteLogDir, null, segment, storage)).isNull();
        copyExecutor.triggerAll();
        assertThat(otherServer.copyFetchInfo(remoteLogDir, null, segment, storage)).isNotNull();
    }

    @Test
    void testFailedCopyIsRetriedLater() throws Exception {
        assertThat(originalFile().delete()).isTrue();
        assertThat(copies.copyFetchInfo(remoteLogDir, null, segment, storage)).isNull();
        copyExecutor.triggerAll();

        // the copy failed, so the next fetches don't try again right away
        assertThat(copies.copyFetchInfo(remoteLogDir, null, segment, storage)).isNull();
        assertThat(copyExecutor.numQueuedRunnables()).isEqualTo(0);

        clock.advanceTime(Duration.ofMinutes(1));
        assertThat(copies.copyFetchInfo(remoteLogDir, null, segment, storage)).isNull();
        assertThat(copyExecutor.numQueuedRunnables()).isEqualTo(1);
    }

    @Test
    void testCopyIsDeletedWithItsSegment() throws Exception {
        assertThat(copies.copyFetchInfo(remoteLogDir, null, segment, storage)).isNull();
        copyExecutor.triggerAll();
        assertThat(localFile(RemoteLogCompatCopies.copyFile(segmentDir, segment))).exists();

        storage.deleteLogSegmentFiles(remoteLogDir, segment);
        assertThat(localFile(segmentDir)).doesNotExist();
    }

    private File originalFile() {
        return localFile(remoteLogSegmentFile(segmentDir, segment.remoteLogStartOffset()));
    }

    private static File localFile(FsPath path) {
        return new File(path.toUri().getPath());
    }

    private static List<Long> baseOffsets(File file) throws Exception {
        List<Long> offsets = new ArrayList<>();
        try (FileLogRecords records = FileLogRecords.open(file)) {
            for (LogRecordBatch batch : records.batches()) {
                offsets.add(batch.baseLogOffset());
            }
        }
        return offsets;
    }
}
