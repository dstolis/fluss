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

package org.apache.fluss.client.write;

import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.rpc.protocol.feature.ClusterFeature;
import org.apache.fluss.server.log.WriterStateEntry;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * With V3 writes enabled, a writer whose state the server expired bumps its epoch and keeps
 * writing, instead of failing with {@link OutOfOrderSequenceException}.
 */
class WriterEpochITCase extends WriterStateExpirationITCaseBase {

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConf())
                    .build();

    @BeforeAll
    static void enableV3Writes() throws Exception {
        // before the writer of the test connects, so it is told to write V3 when it connects
        FLUSS_CLUSTER_EXTENSION.finalizeClusterFeature(ClusterFeature.LOG_RECORD_BATCH_VERSION, 3);
    }

    @Override
    protected FlussClusterExtension cluster() {
        return FLUSS_CLUSTER_EXTENSION;
    }

    @Test
    void testWriterBumpsEpochAfterWriterStateExpired() throws Exception {
        assertThat(writeAcrossWriterStateExpiration()).isNull();

        int total = ROWS_BEFORE_EXPIRATION + ROWS_AFTER_EXPIRATION;
        // every row is stored once, in order
        assertThat(readAllRows(total))
                .containsExactlyElementsOf(
                        IntStream.range(0, total).boxed().collect(Collectors.toList()));

        // the leader and both followers know the writer again, under epoch 1, where its
        // sequence restarted at 0
        retry(
                Duration.ofMinutes(1),
                () -> {
                    List<WriterStateEntry> states = writerStates();
                    assertThat(states).hasSize(3);
                    for (WriterStateEntry state : states) {
                        assertThat(state.writerEpoch()).isEqualTo((short) 1);
                        assertThat(state.lastBatchSequence()).isEqualTo(ROWS_AFTER_EXPIRATION - 1);
                    }
                });
    }
}
