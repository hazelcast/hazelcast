/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.json.WriterConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DataInputNavigableJsonAdapter;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NavigableJsonInputAdapter;
import com.hazelcast.internal.serialization.impl.StringNavigableJsonAdapter;
import com.hazelcast.query.impl.getters.JsonPathCursor;

import java.io.IOException;
import java.util.List;

import static com.hazelcast.query.impl.getters.AbstractJsonGetter.getPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class AbstractJsonSchemaTest {

    protected final int JSON_UTF8_START_OFFSET = 12; // 8 byte (header) + 4 byte (character count)

    protected JsonFactory factory = new JsonFactory();

    protected InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    protected abstract InMemoryFormat getInMemoryFormay();

    protected JsonParser createParserFromInput(NavigableJsonInputAdapter input) throws IOException {
        return input.createParser(factory);
    }

    protected NavigableJsonInputAdapter toAdapter(HazelcastJsonValue jsonValue) {
        if (getInMemoryFormay() == InMemoryFormat.OBJECT) {
            return new StringNavigableJsonAdapter(jsonValue.toString(), 0);
        } else {
            return new DataInputNavigableJsonAdapter(serializationService.createObjectDataInput(serializationService.toData(jsonValue)), JSON_UTF8_START_OFFSET);
        }
    }

    protected void testPaths(WriterConfig config) throws IOException {
        for (JsonValue value : TestJsonValues.LIST) {
            String jsonString = value.toString(config);
            validateJson(jsonString, value);
        }
    }


    protected void validateJson(String originalString, JsonValue jsonValue) throws IOException {
        NavigableJsonInputAdapter input = toAdapter(new HazelcastJsonValue(originalString));
        JsonSchemaNode description = JsonSchemaHelper.createSchema(createParserFromInput(input));
        if (jsonValue.isObject()) {
            validateJsonObject(originalString, new JsonPattern(), jsonValue.asObject(), description, input, null);
        } else if (jsonValue.isArray()) {
            validateJsonArray(originalString, new JsonPattern(), jsonValue.asArray(), description, input, null);
        } else {
            // shoult not reach here
            fail();
        }
    }

    private void validateJsonObject(String originalString, JsonPattern patternPrefix, JsonObject jsonObject,
                                    JsonSchemaNode description, NavigableJsonInputAdapter input,
                                    String pathPrefix) throws IOException {
        List<String> attributeNames = jsonObject.names();
        for (int i = 0; i < attributeNames.size(); i++) {
            String attributeName = attributeNames.get(i);
            JsonValue attribute = jsonObject.get(attributeName);
            JsonPattern copy =  new JsonPattern(patternPrefix);
            copy.add(i);
            String nextPath = attributeName;
            if (pathPrefix != null) {
                nextPath = pathPrefix + "." + attributeName;
            }
            if (attribute.isObject()) {
                validateJsonObject(originalString, copy, attribute.asObject(), description, input, nextPath);
            } else if (attribute.isArray()) {
                validateJsonArray(originalString, copy, attribute.asArray(), description, input, nextPath);
            } else {
                validateJsonLiteral(attribute, originalString, copy, description, input, nextPath);
            }
        }
    }

    private void validateJsonArray(String originalString, JsonPattern patternPrefix, JsonArray jsonArray,
                                   JsonSchemaNode description, NavigableJsonInputAdapter input, String pathPrefix)
            throws IOException {
        for (int i = 0; i < jsonArray.size(); i++) {
            JsonValue item = jsonArray.get(i);
            JsonPattern copy = new JsonPattern(patternPrefix);
            copy.add(i);
            String nextPath = "[" + i + "]";
            if (pathPrefix != null) {
                nextPath = pathPrefix + nextPath;
            }
            if (item.isObject()) {
                validateJsonObject(originalString, copy, item.asObject(), description, input, nextPath);
            } else if (item.isArray()) {
                validateJsonArray(originalString, copy, item.asArray(), description, input, nextPath);
            } else {
                validateJsonLiteral(item, originalString, copy, description, input, nextPath);
            }
        }
    }

    protected void validateJsonLiteral(JsonValue expectedValue, String originalString,
                                       JsonPattern expectedPattern,
                                       JsonSchemaNode description,
                                       NavigableJsonInputAdapter input,
                                       String path) throws IOException {
        JsonPathCursor pathArray = splitPath(path);
        JsonPattern pattern = JsonSchemaHelper.createPattern(input, description, pathArray);
        String failureString = String.format("Path ( %s ) failed on ( %s )", path, originalString);
        assertEquals(failureString, expectedPattern, pattern);
        pathArray.reset();
        input.reset();
        JsonValue foundValue = JsonSchemaHelper.findValueWithPattern(input, description, pattern, pathArray);
        assertEquals(failureString, expectedValue, foundValue);
    }

    JsonPathCursor splitPath(String attributePath) {
        return getPath(attributePath);
    }
}
