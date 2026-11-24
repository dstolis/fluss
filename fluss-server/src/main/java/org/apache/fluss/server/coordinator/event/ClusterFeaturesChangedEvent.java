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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.rpc.protocol.feature.ClusterFeatureLevels;

/**
 * An event for a change of the finalized cluster feature levels, raised on the coordinator when it
 * applies the change. It runs on the coordinator's event thread, like the registration of tablet
 * servers, so a tablet server that registers while the level is raised is either seen by the check
 * before the raise or handled by this event.
 */
public class ClusterFeaturesChangedEvent implements CoordinatorEvent {

    private final ClusterFeatureLevels levels;

    public ClusterFeaturesChangedEvent(ClusterFeatureLevels levels) {
        this.levels = levels;
    }

    public ClusterFeatureLevels getLevels() {
        return levels;
    }

    @Override
    public String toString() {
        return "ClusterFeaturesChangedEvent{levels=" + levels + '}';
    }
}
