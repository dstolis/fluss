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
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Without V3 writes, batches carry no writer epoch, so a writer whose state the server expired
 * fails with {@link OutOfOrderSequenceException}, as before this change.
 */
class WriterStateExpirationWithoutV3ITCase extends WriterStateExpirationITCaseBase {

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(clusterConf())
                    .build();

    @Override
    protected FlussClusterExtension cluster() {
        return FLUSS_CLUSTER_EXTENSION;
    }

    @Test
    void testWriteFailsAfterWriterStateExpired() throws Exception {
        assertThat(writeAcrossWriterStateExpiration())
                .isInstanceOf(OutOfOrderSequenceException.class);
    }
}
