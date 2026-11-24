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

import org.apache.fluss.exception.UnreadableClusterFeatureLevelsException;
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.server.zk.ZooKeeperClient.VersionedData;
import org.apache.fluss.server.zk.data.AppliedClusterFeatures;
import org.apache.fluss.server.zk.data.ZkData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.rpc.protocol.feature.ClusterFeature.LOG_RECORD_BATCH_VERSION;
import static org.apache.fluss.server.zk.data.ZkData.ClusterFeaturesZNode.APPLIED_LEVELS_FILE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that the finalized cluster feature levels never go down when their ZooKeeper node is lost,
 * rolled back or corrupt.
 */
class ClusterFeatureLevelsRecoveryTest {

    private static final ClusterFeatureLevels BASELINE = ClusterFeatureLevels.baseline();
    private static final ClusterFeatureLevels RAISED = ClusterFeatureLevels.latest();

    @Test
    void testServerStartsWithTheStoredLevels() throws Exception {
        VersionedData<ClusterFeatureLevels> start =
                ClusterFeatures.levelsToStartWith(() -> stored(RAISED, 4), Optional.of(BASELINE));
        assertThat(start.data()).isEqualTo(RAISED);
        assertThat(start.zkVersion()).isEqualTo(4);

        // a new server, or one of a cluster that never raised a level
        start = ClusterFeatures.levelsToStartWith(Optional::empty, Optional.empty());
        assertThat(start.data()).isEqualTo(BASELINE);
        assertThat(start.zkVersion()).isEqualTo(FinalizedClusterFeatures.NO_EPOCH);
    }

    @Test
    void testServerRefusesToStartBelowALevelItApplied() {
        // the node was deleted
        assertThatThrownBy(
                        () ->
                                ClusterFeatures.levelsToStartWith(
                                        Optional::empty, Optional.of(RAISED)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("are missing")
                .hasMessageContaining("Restore the node");
        // the node was rolled back to a lower level
        assertThatThrownBy(
                        () ->
                                ClusterFeatures.levelsToStartWith(
                                        () -> stored(BASELINE, 0), Optional.of(RAISED)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Levels never go down");
    }

    @Test
    void testServerStartsWithItsOwnLevelsWhileTheNodeIsCorrupt() throws Exception {
        VersionedData<ClusterFeatureLevels> start =
                ClusterFeatures.levelsToStartWith(
                        ClusterFeatureLevelsRecoveryTest::corrupt, Optional.of(RAISED));
        assertThat(start.data()).isEqualTo(RAISED);
        assertThat(start.zkVersion()).isEqualTo(FinalizedClusterFeatures.NO_EPOCH);

        // a server that never applied levels can't know them
        assertThatThrownBy(
                        () ->
                                ClusterFeatures.levelsToStartWith(
                                        ClusterFeatureLevelsRecoveryTest::corrupt,
                                        Optional.empty()))
                .isInstanceOf(UnreadableClusterFeatureLevelsException.class)
                .hasMessageContaining("/cluster/features")
                .hasMessageContaining("Set the node to the levels the cluster had");
    }

    @Test
    void testCorruptNodeIsReportedWithHowToRepairIt() {
        for (String content :
                Arrays.asList(
                        "not-json{",
                        "{\"version\":1}",
                        "{\"version\":1,\"levels\":{\"log.record-batch.version\":\"x\"}}")) {
            assertThatThrownBy(
                            () ->
                                    ZkData.ClusterFeaturesZNode.decode(
                                            content.getBytes(StandardCharsets.UTF_8)))
                    .isInstanceOf(UnreadableClusterFeatureLevelsException.class)
                    .hasMessageContaining("can't be read")
                    .hasMessageContaining(APPLIED_LEVELS_FILE_NAME);
        }
    }

    @Test
    void testAppliedLevelsFileKeepsTheHighestLevels(@TempDir File dir1, @TempDir File dir2)
            throws Exception {
        assertThat(AppliedClusterFeatureLevelsFile.read(Arrays.asList(dir1, dir2))).isEmpty();

        AppliedClusterFeatureLevelsFile.write(Arrays.asList(dir1, dir2), RAISED);
        assertThat(AppliedClusterFeatureLevelsFile.read(Arrays.asList(dir1, dir2)))
                .contains(RAISED);

        // a directory added later, or one that lost its file, does not lower the levels
        AppliedClusterFeatureLevelsFile.write(Collections.singletonList(dir2), BASELINE);
        assertThat(AppliedClusterFeatureLevelsFile.read(Arrays.asList(dir1, dir2)))
                .contains(RAISED);
        assertThat(new File(dir1, APPLIED_LEVELS_FILE_NAME + ".tmp")).doesNotExist();

        Files.write(new File(dir2, APPLIED_LEVELS_FILE_NAME).toPath(), new byte[] {'{'});
        assertThatThrownBy(() -> AppliedClusterFeatureLevelsFile.read(Arrays.asList(dir1, dir2)))
                .hasMessageContaining("can't be read");
    }

    @Test
    void testCoordinatorRefusesToStartBelowALevelATabletServerApplied() {
        Map<Integer, AppliedClusterFeatures> applied =
                Collections.singletonMap(
                        3,
                        new AppliedClusterFeatures(
                                1,
                                Collections.singletonMap(
                                        LOG_RECORD_BATCH_VERSION.featureName(), 3)));
        assertThatThrownBy(
                        () ->
                                ClusterFeatureManager.checkNoTabletServerAppliedMore(
                                        BASELINE, applied))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("tablet servers {3=")
                .hasMessageContaining("Restore the node");

        // servers at the initial levels don't stop it
        ClusterFeatureManager.checkNoTabletServerAppliedMore(RAISED, applied);
        ClusterFeatureManager.checkNoTabletServerAppliedMore(BASELINE, Collections.emptyMap());
    }

    private static Optional<VersionedData<Map<String, Integer>>> stored(
            ClusterFeatureLevels levels, int epoch) {
        return Optional.of(new VersionedData<>(levels.toMap(), epoch));
    }

    private static Optional<VersionedData<Map<String, Integer>>> corrupt() {
        return Optional.of(
                new VersionedData<>(
                        ZkData.ClusterFeaturesZNode.decode(
                                "not-json{".getBytes(StandardCharsets.UTF_8)),
                        0));
    }
}
