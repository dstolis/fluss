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

import org.apache.fluss.server.metadata.ServerInfo;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/** An event for new tablet server. */
public class NewTabletServerEvent implements CoordinatorEvent {

    private final ServerInfo serverInfo;
    // the cluster features the server registered, mapped to the highest supported level
    private final Map<String, Integer> supportedFeatures;

    public NewTabletServerEvent(ServerInfo serverInfo) {
        this(serverInfo, Collections.emptyMap());
    }

    public NewTabletServerEvent(ServerInfo serverInfo, Map<String, Integer> supportedFeatures) {
        this.serverInfo = serverInfo;
        this.supportedFeatures = supportedFeatures;
    }

    public ServerInfo getServerInfo() {
        return serverInfo;
    }

    /** The cluster features the server registered, mapped to the highest supported level. */
    public Map<String, Integer> getSupportedFeatures() {
        return supportedFeatures;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NewTabletServerEvent that = (NewTabletServerEvent) o;
        return Objects.equals(serverInfo, that.serverInfo)
                && Objects.equals(supportedFeatures, that.supportedFeatures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverInfo, supportedFeatures);
    }

    @Override
    public String toString() {
        return "NewTabletServerEvent{"
                + "serverInfo="
                + serverInfo
                + ", supportedFeatures="
                + supportedFeatures
                + '}';
    }
}
