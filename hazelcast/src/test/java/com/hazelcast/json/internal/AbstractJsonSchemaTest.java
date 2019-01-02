/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.json.internal;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.json.WriterConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.json.HazelcastJson;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.query.impl.getters.AbstractJsonGetter.getPath;
import static org.junit.Assert.assertEquals;

public abstract class AbstractJsonSchemaTest {

    private JsonFactory factory = new JsonFactory();

    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    protected JsonParser createParserFromString(String jsonString) throws IOException {
        byte[] bytes = jsonString.getBytes(Charset.forName("UTF8"));
        return factory.createParser(bytes, 0, bytes.length);
    }

    protected void printWithGuides(String str) {
        StringBuilder builder = new StringBuilder();
        int l = str.length();
        if (l > 10) {
            for (int i = 0; i < l; i++) {
                builder.append(i % 10 == 0 ? ((i / 10) % 10) : " ");
            }
            builder.append('\n');
        }
        for (int i = 0; i < l; i++) {
            builder.append(i % 10);
        }
        builder.append('\n');
        builder.append(str);
        System.out.println(builder.toString());
    }

    protected void testPaths(WriterConfig config) throws IOException {
        for (JsonValue value : TestJsonValues.LIST) {
            String jsonString = value.toString(config);
            validateJson(jsonString, value);
        }
    }

    protected void testOne(int index) throws IOException {
        String jsonString = TestJsonValues.LIST.get(index).toString(WriterConfig.MINIMAL);
        validateJson(jsonString, TestJsonValues.LIST.get(index));
    }

    private ObjectDataInput serializeAndSkipHeader(HazelcastJsonValue jsonValue) {
        return serializationService.createObjectDataInput(serializationService.toBytes(jsonValue));
    }

    protected void validateJson(String originalString, JsonValue jsonValue) throws IOException {
        System.out.println("Testing " + originalString);
        BufferObjectDataInput input = (BufferObjectDataInput) serializeAndSkipHeader(HazelcastJson.fromString(originalString));
        JsonSchemaDescription description = JsonSchemaHelper.createDescription(createParserFromString(originalString));
        if (jsonValue.isObject()) {
            validateJsonObject(originalString, new ArrayList<Integer>(), jsonValue.asObject(), description, input, null);
        } else if (jsonValue.isArray()) {
            validateJsonArray(originalString, new ArrayList<Integer>(), jsonValue.asArray(), description, input, null);
        } else {
            validateJsonLiteral(jsonValue, originalString, Collections.EMPTY_LIST, description, input, "this");
        }
    }

    private void validateJsonObject(String originalString, List<Integer> patternPrefix, JsonObject jsonObject, JsonSchemaDescription description, BufferObjectDataInput input, String pathPrefix) throws IOException {
        List<String> attributeNames = jsonObject.names();
        for (int i = 0; i < attributeNames.size(); i++) {
            String attributeName = attributeNames.get(i);
            JsonValue attribute = jsonObject.get(attributeName);
            patternPrefix.add(i);
            String nextPath = attributeName;
            if (pathPrefix != null) {
                nextPath = pathPrefix + "." + attributeName;
            }
            if (attribute.isObject()) {
                validateJsonObject(originalString, patternPrefix, attribute.asObject(), description, input, nextPath);
            } else if (attribute.isArray()) {
                validateJsonArray(originalString, patternPrefix, attribute.asArray(), description, input, nextPath);
            } else {
                validateJsonLiteral(attribute, originalString, patternPrefix, description, input, nextPath);
            }
            patternPrefix.remove(patternPrefix.size() - 1);
        }
    }

    private void validateJsonArray(String originalString, List<Integer> patternPrefix, JsonArray jsonArray, JsonSchemaDescription description, BufferObjectDataInput input, String pathPrefix) throws IOException {
        for (int i = 0; i < jsonArray.size(); i++) {
            JsonValue item = jsonArray.get(i);
            patternPrefix.add(i);
            String nextPath = "[" + i + "]";
            if (pathPrefix != null) {
                nextPath = pathPrefix + nextPath;
            }
            if (item.isObject()) {
                validateJsonObject(originalString, patternPrefix, item.asObject(), description, input, nextPath);
            } else if (item.isArray()) {
                validateJsonArray(originalString, patternPrefix, item.asArray(), description, input, nextPath);
            } else {
                validateJsonLiteral(item, originalString, patternPrefix, description, input, nextPath);
            }
            patternPrefix.remove(patternPrefix.size() - 1);
        }
    }

    protected void validateJsonLiteral(JsonValue expectedValue, String originalString,
                                       List<Integer> expectedPattern,
                                       JsonSchemaDescription description,
                                       BufferObjectDataInput input,
                                       String path) throws IOException {
        String[] pathArray = splitPath(path);
        List<Integer> pattern = JsonSchemaHelper.findPattern(input, 12, description, pathArray);
        String failureString = String.format("Path ( %s ) failed on ( %s )", path, originalString);
        assertEquals(failureString, expectedPattern, pattern);
        input.reset();
        JsonValue foundValue = JsonSchemaHelper.findValueWithPattern(input, 12, description, pattern, pathArray);
        assertEquals(failureString, expectedValue, foundValue);
        System.out.println("checked path: " + path + " (" + Arrays.toString(pattern.toArray()) + ")");
    }

    String[] splitPath(String attributePath) {
        return getPath(attributePath);
    }
}
