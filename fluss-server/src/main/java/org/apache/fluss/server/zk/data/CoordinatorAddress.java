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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.rpc.protocol.feature.SupportedLevelRange;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The address information of an active coordinator stored in {@link ZkData.CoordinatorLeaderZNode}.
 *
 * @see CoordinatorAddressJsonSerde for json serialization and deserialization.
 */
public class CoordinatorAddress {
    private final String id;
    private final List<Endpoint> endpoints;
    // cluster feature name -> range of levels this coordinator supports
    private final Map<String, SupportedLevelRange> supportedFeatures;

    public CoordinatorAddress(String id, List<Endpoint> endpoints) {
        this(id, endpoints, Collections.emptyMap());
    }

    public CoordinatorAddress(
            String id,
            List<Endpoint> endpoints,
            Map<String, SupportedLevelRange> supportedFeatures) {
        this.id = id;
        this.endpoints = endpoints;
        this.supportedFeatures = Collections.unmodifiableMap(new TreeMap<>(supportedFeatures));
    }

    public String getId() {
        return id;
    }

    public List<Endpoint> getEndpoints() {
        return endpoints;
    }

    /**
     * The cluster features this coordinator supports, mapped to the range of supported levels. It
     * is empty for coordinators that registered before cluster features existed, which support the
     * baseline level of every feature.
     */
    public Map<String, SupportedLevelRange> getSupportedFeatures() {
        return supportedFeatures;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CoordinatorAddress that = (CoordinatorAddress) o;
        return Objects.equals(id, that.id)
                && Objects.equals(endpoints, that.endpoints)
                && Objects.equals(supportedFeatures, that.supportedFeatures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, endpoints, supportedFeatures);
    }

    @Override
    public String toString() {
        return "CoordinatorAddress{"
                + "id='"
                + id
                + '\''
                + ", endpoints="
                + endpoints
                + ", supportedFeatures="
                + supportedFeatures
                + '}';
    }
}
