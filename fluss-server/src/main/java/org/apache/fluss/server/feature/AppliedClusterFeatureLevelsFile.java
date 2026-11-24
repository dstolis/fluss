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
import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;
import org.apache.fluss.server.zk.data.ClusterFeatureLevelsJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.server.zk.data.ZkData.ClusterFeaturesZNode.APPLIED_LEVELS_FILE_NAME;

/**
 * The cluster feature levels a tablet server has applied, kept in each of its data directories.
 *
 * <p>Data written at an applied level may exist on the server's disks, so the server must never
 * start with a lower level, even if the levels stored in ZooKeeper were lost or rolled back. The
 * file lets it notice, see {@link ClusterFeatures#levelsToStartWith}. The highest levels found in
 * any directory count, so a directory that was added later or lost its file does not lower them.
 */
@Internal
public final class AppliedClusterFeatureLevelsFile {

    private static final String TEMPORARY_SUFFIX = ".tmp";

    private AppliedClusterFeatureLevelsFile() {}

    /** The highest levels kept in the given directories, empty if none keeps any. */
    public static Optional<ClusterFeatureLevels> read(List<File> dataDirs) throws IOException {
        ClusterFeatureLevels highest = null;
        for (File dir : dataDirs) {
            File file = new File(dir, APPLIED_LEVELS_FILE_NAME);
            if (!file.exists()) {
                continue;
            }
            ClusterFeatureLevels levels;
            try {
                levels =
                        ClusterFeatureLevels.fromMap(
                                JsonSerdeUtils.readValue(
                                        Files.readAllBytes(file.toPath()),
                                        ClusterFeatureLevelsJsonSerde.INSTANCE));
            } catch (Exception e) {
                throw new IOException(
                        String.format(
                                "The cluster feature levels in %s can't be read. Delete the "
                                        + "file only if this tablet server never wrote data at a "
                                        + "raised level.",
                                file),
                        e);
            }
            highest = highest == null ? levels : highest.max(levels);
        }
        return Optional.ofNullable(highest);
    }

    /** Keeps the given levels in every given directory, replacing what was kept before. */
    public static void write(List<File> dataDirs, ClusterFeatureLevels levels) throws IOException {
        byte[] json =
                JsonSerdeUtils.writeValueAsBytes(
                        levels.toMap(), ClusterFeatureLevelsJsonSerde.INSTANCE);
        for (File dir : dataDirs) {
            Path file = new File(dir, APPLIED_LEVELS_FILE_NAME).toPath();
            Path temporary = new File(dir, APPLIED_LEVELS_FILE_NAME + TEMPORARY_SUFFIX).toPath();
            Files.write(temporary, json);
            Files.move(
                    temporary,
                    file,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        }
    }
}
