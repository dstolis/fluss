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
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Json serializer and deserializer for the finalized cluster feature levels, a map of feature name
 * to level. Only features above their baseline level are stored.
 */
public class ClusterFeatureLevelsJsonSerde
        implements JsonSerializer<Map<String, Integer>>, JsonDeserializer<Map<String, Integer>> {

    public static final ClusterFeatureLevelsJsonSerde INSTANCE =
            new ClusterFeatureLevelsJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String LEVELS = "levels";
    private static final int VERSION = 1;

    @Override
    public void serialize(Map<String, Integer> levels, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeObjectFieldStart(LEVELS);
        for (Map.Entry<String, Integer> level : new TreeMap<>(levels).entrySet()) {
            generator.writeNumberField(level.getKey(), level.getValue());
        }
        generator.writeEndObject();
        generator.writeEndObject();
    }

    @Override
    public Map<String, Integer> deserialize(JsonNode node) {
        JsonNode levelsNode = node.get(LEVELS);
        if (levelsNode == null || !levelsNode.isObject()) {
            throw new IllegalArgumentException("no \"" + LEVELS + "\" object");
        }
        Map<String, Integer> levels = new TreeMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = levelsNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (!field.getValue().isInt()) {
                throw new IllegalArgumentException(
                        "the level of " + field.getKey() + " is not a number");
            }
            levels.put(field.getKey(), field.getValue().asInt());
        }
        return levels;
    }
}
