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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.rpc.protocol.feature.SupportedLevelRange;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Writes and reads the supported cluster features of a server registration, a map of feature name
 * to the supported range, as in {@code {"log.record-batch.version": {"min": 2, "max": 3}}}.
 */
@Internal
final class SupportedFeaturesJson {

    private static final String MIN = "min";
    private static final String MAX = "max";

    private SupportedFeaturesJson() {}

    static void write(
            String fieldName,
            Map<String, SupportedLevelRange> supportedFeatures,
            JsonGenerator generator)
            throws IOException {
        if (supportedFeatures.isEmpty()) {
            return;
        }
        generator.writeObjectFieldStart(fieldName);
        for (Map.Entry<String, SupportedLevelRange> feature :
                new TreeMap<>(supportedFeatures).entrySet()) {
            generator.writeObjectFieldStart(feature.getKey());
            generator.writeNumberField(MIN, feature.getValue().min());
            generator.writeNumberField(MAX, feature.getValue().max());
            generator.writeEndObject();
        }
        generator.writeEndObject();
    }

    static Map<String, SupportedLevelRange> read(JsonNode features) {
        Map<String, SupportedLevelRange> supportedFeatures = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = features.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> feature = fields.next();
            JsonNode range = feature.getValue();
            supportedFeatures.put(
                    feature.getKey(),
                    new SupportedLevelRange(range.get(MIN).asInt(), range.get(MAX).asInt()));
        }
        return supportedFeatures;
    }
}
