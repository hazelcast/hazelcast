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

package com.hazelcast.query.impl.getters;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.serialization.impl.NavigableJsonInputAdapter;
import com.hazelcast.json.internal.JsonPattern;
import com.hazelcast.json.internal.JsonSchemaHelper;
import com.hazelcast.json.internal.JsonSchemaNode;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractJsonGetter extends Getter {

    private Map<String, JsonPattern> patternMap = new ConcurrentHashMap<String, JsonPattern>();

    AbstractJsonGetter() {
        super(null);
    }

    AbstractJsonGetter(Getter parent) {
        super(parent);
    }

    /**
     * Converts a JsonValue object to its corresponding Java object.
     * Type mappings are as shown below
     * <ul>
     *     <li>{@link com.hazelcast.internal.json.JsonString} to {@link String}</li>
     *     <li>{@link com.hazelcast.internal.json.Json.NULL} to {@code null}</li>
     *     <li>{@link com.hazelcast.internal.json.Json.TRUE} to {@code true}</li>
     *     <li>{@link com.hazelcast.internal.json.Json.FALSE} to {@code false}</li>
     *     <li>
     *         {@link com.hazelcast.internal.json.JsonNumber} to either
     *         {@code long} or {@code double}
     *     </li>
     * </ul>
     * @param value
     * @return
     */
    public static Object convertFromJsonValue(JsonValue value) {
        if (value == null) {
            return null;
        } else if (value.isNumber()) {
            if (value.toString().contains(".")) {
                return value.asDouble();
            } else {
                return value.asLong();
            }
        } else if (value.isBoolean()) {
            return value.asBoolean();
        } else if (value.isNull()) {
            return null;
        } else if (value.isString()) {
            return value.asString();
        }
        throw new IllegalArgumentException("Unknown Json type: " + value);
    }

    abstract JsonParser createParser(Object obj) throws IOException;

    @Override
    Object getValue(Object obj) {
        throw new HazelcastException("Path agnostic value extraction is not supported");

    }

    @Override
    Object getValue(Object obj, String attributePath) throws Exception {
        JsonPathCursor pathCursor = getPath(attributePath);
        JsonParser parser = createParser(obj);

        try {
            parser.nextToken();
            while (pathCursor.getNext() != null) {
                if (!pathCursor.isArray()) {
                    // non array case
                    if (!findAttribute(parser, pathCursor)) {
                        return null;
                    }
                } else {
                    // array case
                    if (pathCursor.isAny()) {
                        return getMultiValue(parser, pathCursor);
                    } else {
                        JsonToken token = parser.currentToken();
                        if (token != JsonToken.START_ARRAY) {
                            return null;
                        }
                        token = parser.nextToken();
                        int arrayIndex = pathCursor.getArrayIndex();
                        for (int j = 0; j < arrayIndex; j++) {
                            if (token == JsonToken.END_ARRAY) {
                                return null;
                            }
                            parser.skipChildren();
                            token = parser.nextToken();
                        }
                    }
                }
            }
            return convertJsonTokenToValue(parser);
        } finally {
            parser.close();
        }
    }

    JsonPattern getOrCreatePattern(NavigableJsonInputAdapter adapter,
                                   JsonSchemaNode jsonSchemaNode,
                                   JsonPathCursor pathCursor) {
        JsonPattern knownPattern = patternMap.get(pathCursor.getAttributePath());
        if (knownPattern == null) {
            knownPattern = JsonSchemaHelper.createPattern(adapter, jsonSchemaNode, pathCursor);
            if (knownPattern != null) {
                patternMap.put(pathCursor.getAttributePath(), knownPattern);
            }
            pathCursor.reset();
        }
        return knownPattern;
    }

    @Override
    Object getValue(Object obj, String attributePath, Object metadata) throws Exception {
        if (metadata == null) {
            return getValue(obj, attributePath);
        }
        JsonSchemaNode schemaNode = (JsonSchemaNode) metadata;
        JsonPathCursor pathCursor = getPath(attributePath);

        NavigableJsonInputAdapter adapter = annotate(obj);
        JsonPattern knownPattern = getOrCreatePattern(adapter, schemaNode, pathCursor);
        if (knownPattern == null) {
            return null;
        }
        if (knownPattern.hasAny()) {
            return getValue(obj, attributePath);
        }
        return JsonSchemaHelper.findValueWithPattern(adapter, schemaNode, knownPattern, pathCursor);
    }

    @Override
    Class getReturnType() {
        throw new IllegalArgumentException("Non applicable for Json getters");
    }

    @Override
    boolean isCacheable() {
        return false;
    }

    protected abstract NavigableJsonInputAdapter annotate(Object object);

    /**
     * Looks for the attribute with the given name only in current
     * object. If found, parser points to the value of the given
     * attribute when this method returns. If given path does not exist
     * in the current level, then parser points to matching
     * {@code JsonToken.END_OBJECT} of the current object.
     *
     * Assumes the parser points to a {@code JsonToken.START_OBJECT}
     *
     * @param parser
     * @param pathCursor
     * @return {@code true} if given attribute name exists in the current object
     * @throws IOException
     */
    private boolean findAttribute(JsonParser parser, JsonPathCursor pathCursor) throws IOException {
        JsonToken token = parser.getCurrentToken();
        if (token != JsonToken.START_OBJECT) {
            return false;
        }
        while (true) {
            token = parser.nextToken();
            if (token == JsonToken.END_OBJECT) {
                return false;
            }
            if (pathCursor.getCurrent().equals(parser.getCurrentName())) {
                parser.nextToken();
                return true;
            } else {
                parser.nextToken();
                parser.skipChildren();
            }
        }
    }

    /**
     * Traverses given array. If {@code cursor#getNext()} is
     * {@code null}, this method adds all the scalar values in current
     * array to the result. Otherwise, it traverses all objects in
     * given array and adds their scalar values named
     * {@code cursor#getNext()} to the result.
     *
     * Assumes the parser points to an array.
     *
     * @param parser
     * @param cursor
     * @return All matches in the current array that conform to
     *          [any].lastPath search
     * @throws IOException
     */
    private MultiResult getMultiValue(JsonParser parser, JsonPathCursor cursor) throws IOException {
        cursor.getNext();
        MultiResult<Object> multiResult = new MultiResult<Object>();

        JsonToken currentToken = parser.currentToken();
        if (currentToken != JsonToken.START_ARRAY) {
            return null;
        }
        while (true) {
            currentToken = parser.nextToken();
            if (currentToken == JsonToken.END_ARRAY) {
                break;
            }
            if (cursor.getCurrent() == null) {
                if (currentToken.isScalarValue()) {
                    multiResult.add(convertJsonTokenToValue(parser));
                } else {
                    parser.skipChildren();
                }
            } else {
                if (currentToken == JsonToken.START_OBJECT && findAttribute(parser, cursor)) {
                    multiResult.add(convertJsonTokenToValue(parser));
                    while (parser.getCurrentToken() != JsonToken.END_OBJECT) {
                        if (parser.currentToken().isStructStart()) {
                            parser.skipChildren();
                        }
                        parser.nextToken();
                    }
                } else if (currentToken == JsonToken.START_ARRAY) {
                    parser.skipChildren();
                }
            }
        }
        return multiResult;
    }

    private Object convertJsonTokenToValue(JsonParser parser) throws IOException {
        int token = parser.currentTokenId();
        switch (token) {
            case JsonTokenId.ID_STRING:
                return parser.getValueAsString();
            case JsonTokenId.ID_NUMBER_INT:
                return parser.getIntValue();
            case JsonTokenId.ID_NUMBER_FLOAT:
                return parser.getValueAsDouble();
            case JsonTokenId.ID_TRUE:
                return true;
            case JsonTokenId.ID_FALSE:
                return false;
            default:
                return null;
        }
    }

    public static JsonPathCursor getPath(String attributePath) {
        return new JsonPathCursor(attributePath);
    }
}
