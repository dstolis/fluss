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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.log.FetchParams;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V3;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsV3;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests how {@link ReplicaManager} serves fetches of readers below the finalized batch level. */
class ReplicaManagerFinalizedLevelTest extends ReplicaTestBase {

    @Test
    void testOldReaderFetchParkedAcrossARaiseGetsConvertedBatches() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(tb.getBucket());
        CompletableFuture<List<ProduceLogResultForBucket>> appended = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(tb, genMemoryLogRecordsByObject(DATA1)),
                null,
                appended::complete);
        assertThat(appended.get()).containsOnly(new ProduceLogResultForBucket(tb, 0, 10L));

        // a reader of batches up to V2 waits for new data while the cluster is at level 2
        FetchParams params = new FetchParams(-1, Integer.MAX_VALUE, 1, 60_000L);
        params.setReaderMaxBatchMagic(LOG_MAGIC_VALUE_V2);
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> fetched =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                params,
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 10L, 1024 * 1024)),
                null,
                fetched::complete);
        assertThat(fetched).isNotDone();

        // the level is raised and a V3 batch arrives, which completes the waiting fetch
        replicaManager.getClusterFeatures().apply(ClusterFeatureLevels.latest());
        appended = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(
                        tb, genMemoryLogRecordsV3(DATA1, 1L, (short) 0, 0, false, 0L)),
                null,
                appended::complete);
        assertThat(appended.get()).containsOnly(new ProduceLogResultForBucket(tb, 10L, 20L));

        FetchLogResultForBucket result = fetched.get().get(tb);
        assertThat(result.failed()).isFalse();
        List<Byte> magics = new ArrayList<>();
        for (LogRecordBatch batch : result.records().batches()) {
            magics.add(batch.magic());
        }
        assertThat(magics).isNotEmpty().allMatch(magic -> magic <= LOG_MAGIC_VALUE_V2);
    }

    @Test
    void testFollowerBelowTheFinalizedLevelIsRefused() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 1);
        makeLogTableAsLeader(
                tb, Arrays.asList(TABLET_SERVER_ID, 2), Arrays.asList(TABLET_SERVER_ID, 2), false);
        replicaManager.getClusterFeatures().apply(ClusterFeatureLevels.latest());
        CompletableFuture<List<ProduceLogResultForBucket>> appended = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(
                        tb, genMemoryLogRecordsV3(DATA1, 1L, (short) 0, 0, false, 0L)),
                null,
                appended::complete);
        assertThat(appended.get()).containsOnly(new ProduceLogResultForBucket(tb, 0L, 10L));

        // a follower that reads batches up to V2, for example an older tablet server admitted by
        // an older coordinator, must not get the V3 batch
        FetchParams oldFollower = new FetchParams(2, Integer.MAX_VALUE);
        oldFollower.setReaderMaxBatchMagic(LOG_MAGIC_VALUE_V2);
        FetchLogResultForBucket refused = fetchOnce(oldFollower, tb);
        assertThat(refused.getErrorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
        assertThat(refused.getErrorMessage()).contains("Upgrade the follower");

        FetchParams newFollower = new FetchParams(2, Integer.MAX_VALUE);
        newFollower.setReaderMaxBatchMagic(LOG_MAGIC_VALUE_V3);
        FetchLogResultForBucket served = fetchOnce(newFollower, tb);
        assertThat(served.failed()).isFalse();
        assertThat(served.records().batches().iterator().next().magic())
                .isEqualTo(LOG_MAGIC_VALUE_V3);
    }

    private FetchLogResultForBucket fetchOnce(FetchParams params, TableBucket tb) throws Exception {
        CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> fetched =
                new CompletableFuture<>();
        replicaManager.fetchLogRecords(
                params,
                Collections.singletonMap(tb, new FetchReqInfo(tb.getTableId(), 0L, 1024 * 1024)),
                null,
                fetched::complete);
        return fetched.get().get(tb);
    }
}
