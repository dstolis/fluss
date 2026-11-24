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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The cluster feature levels a tablet server has applied, with the feature epoch they were stored
 * at. Each tablet server reports them in its own ephemeral znode, so the coordinator can tell which
 * servers have caught up with a raise. Unlike the stored levels, every feature is listed, also at
 * its baseline.
 */
public final class AppliedClusterFeatures {

    private final int epoch;
    private final Map<String, Integer> levels;

    public AppliedClusterFeatures(int epoch, Map<String, Integer> levels) {
        this.epoch = epoch;
        this.levels = Collections.unmodifiableMap(new TreeMap<>(levels));
    }

    /** The feature epoch of the applied levels, or -1 if the cluster has stored no levels yet. */
    public int epoch() {
        return epoch;
    }

    /** The applied level of every feature, by feature name. */
    public Map<String, Integer> levels() {
        return levels;
    }

    public byte[] toJsonBytes() {
        return JsonSerdeUtils.writeValueAsBytes(this, Serde.INSTANCE);
    }

    public static AppliedClusterFeatures fromJsonBytes(byte[] json) {
        return JsonSerdeUtils.readValue(json, Serde.INSTANCE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AppliedClusterFeatures that = (AppliedClusterFeatures) o;
        return epoch == that.epoch && levels.equals(that.levels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, levels);
    }

    @Override
    public String toString() {
        return "AppliedClusterFeatures{epoch=" + epoch + ", levels=" + levels + '}';
    }

    /** Json serializer and deserializer for {@link AppliedClusterFeatures}. */
    public static final class Serde
            implements JsonSerializer<AppliedClusterFeatures>,
                    JsonDeserializer<AppliedClusterFeatures> {

        public static final Serde INSTANCE = new Serde();

        private static final String VERSION_KEY = "version";
        private static final String EPOCH = "epoch";
        private static final String LEVELS = "levels";
        private static final int VERSION = 1;

        @Override
        public void serialize(AppliedClusterFeatures applied, JsonGenerator generator)
                throws IOException {
            generator.writeStartObject();
            generator.writeNumberField(VERSION_KEY, VERSION);
            generator.writeNumberField(EPOCH, applied.epoch);
            generator.writeObjectFieldStart(LEVELS);
            for (Map.Entry<String, Integer> level : applied.levels.entrySet()) {
                generator.writeNumberField(level.getKey(), level.getValue());
            }
            generator.writeEndObject();
            generator.writeEndObject();
        }

        @Override
        public AppliedClusterFeatures deserialize(JsonNode node) {
            Map<String, Integer> levels = new TreeMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.get(LEVELS).fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                levels.put(field.getKey(), field.getValue().asInt());
            }
            return new AppliedClusterFeatures(node.get(EPOCH).asInt(), levels);
        }
    }
}
