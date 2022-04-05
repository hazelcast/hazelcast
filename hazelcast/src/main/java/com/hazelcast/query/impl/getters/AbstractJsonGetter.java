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

package com.hazelcast.query.impl.getters;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.json.NonTerminalJsonValue;
import com.hazelcast.internal.serialization.impl.NavigableJsonInputAdapter;
import com.hazelcast.internal.util.collection.WeightedEvictableList.WeightedItem;
import com.hazelcast.json.internal.JsonPattern;
import com.hazelcast.json.internal.JsonSchemaHelper;
import com.hazelcast.json.internal.JsonSchemaNode;

import java.io.IOException;
import java.util.List;

import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;

public abstract class AbstractJsonGetter extends Getter {

    private static final int QUERY_CONTEXT_CACHE_MAX_SIZE = 40;
    private static final int QUERY_CONTEXT_CACHE_CLEANUP_SIZE = 3;

    /**
     * The number of times this getter will try to use previously observed
     * patterns. The getter tries most commonly observed patterns first.
     * If known patterns do not work, then a new pattern is created for
     * this object.
     */
    private static final int PATTERN_TRY_COUNT = 2;

    private final JsonGetterContextCache contextCache =
            new JsonGetterContextCache(QUERY_CONTEXT_CACHE_MAX_SIZE, QUERY_CONTEXT_CACHE_CLEANUP_SIZE);

    AbstractJsonGetter(Getter parent) {
        super(parent);
    }

    public static JsonPathCursor getPath(String attributePath) {
        return JsonPathCursor.createCursor(attributePath);
    }

    /**
     * Converts a JsonValue object to its corresponding Java object.
     * Type mappings are as shown below
     * <ul>
     *     <li>{@link com.hazelcast.internal.json.JsonString} to {@link String}</li>
     *     <li>{@link com.hazelcast.internal.json.Json#NULL} to {@code null}</li>
     *     <li>{@link com.hazelcast.internal.json.Json#TRUE} to {@code true}</li>
     *     <li>{@link com.hazelcast.internal.json.Json#FALSE} to {@code false}</li>
     *     <li>
     *         {@link com.hazelcast.internal.json.JsonNumber} to either
     *         {@code long} or {@code double}
     *     </li>
     * </ul>
     *
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
        } else if (value == NonTerminalJsonValue.INSTANCE) {
            return value;
        }
        throw new IllegalArgumentException("Unknown Json type: " + value);
    }

    abstract JsonParser createParser(Object obj) throws IOException;

    @Override
    Object getValue(Object obj) {
        throw new HazelcastException("Path agnostic value extraction is not supported");

    }

    @Override
    Object getValue(Object obj, String attributePath) {
        JsonPathCursor pathCursor = getPath(attributePath);

        try (JsonParser parser = createParser(obj)) {
            parser.nextToken();
            while (pathCursor.getNext() != null) {
                if (pathCursor.isArray()) {

                    if (pathCursor.isAny()) {
                        return getMultiValue(parser, pathCursor);
                    }

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

                } else {
                    // non array case
                    if (!findAttribute(parser, pathCursor, false)) {
                        return null;
                    }
                }
            }
            return convertJsonTokenToValue(parser);
        } catch (IOException e) {
            // Just return null in case of exception. Json strings are allowed to be invalid.
            return null;
        }
    }

    @Override
    Object getValue(Object obj, String attributePath, Object metadata) throws Exception {
        if (metadata == null) {
            return getValue(obj, attributePath);
        }
        JsonSchemaNode schemaNode = (JsonSchemaNode) metadata;

        NavigableJsonInputAdapter adapter = annotate(obj);
        JsonGetterContext queryContext = contextCache.getContext(attributePath);
        List<WeightedItem<JsonPattern>> patternsSnapshot = queryContext.getPatternListSnapshot();

        JsonPathCursor pathCursor = queryContext.newJsonPathCursor();
        JsonPattern knownPattern;
        for (int i = 0; i < PATTERN_TRY_COUNT && i < patternsSnapshot.size(); i++) {
            WeightedItem<JsonPattern> patternWeightedItem = patternsSnapshot.get(i);
            knownPattern = patternWeightedItem.getItem();
            JsonValue value = JsonSchemaHelper.findValueWithPattern(adapter, schemaNode, knownPattern, pathCursor);
            pathCursor.reset();
            if (value != null) {
                queryContext.voteFor(patternWeightedItem);
                return convertFromJsonValue(value);
            }
        }
        knownPattern = JsonSchemaHelper.createPattern(adapter, schemaNode, pathCursor);
        pathCursor.reset();
        if (knownPattern != null) {
            if (knownPattern.hasAny()) {
                return getValue(obj, attributePath);
            }
            queryContext.addOrVoteForPattern(knownPattern);
            return convertFromJsonValue(JsonSchemaHelper.findValueWithPattern(adapter, schemaNode, knownPattern, pathCursor));
        }
        return null;
    }

    @Override
    Class getReturnType() {
        throw new IllegalArgumentException("Non applicable for Json getters");
    }

    @Override
    boolean isCacheable() {
        return false;
    }

    int getContextCacheSize() {
        return contextCache.getCacheSize();
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
    private boolean findAttribute(JsonParser parser, JsonPathCursor pathCursor,
                                  boolean multiValue) throws IOException {
        JsonToken token = parser.getCurrentToken();
        if (token != START_OBJECT) {
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
            } else if (multiValue) {
                parser.nextToken();
            } else {
                parser.nextToken();
                parser.skipChildren();
            }
        }
    }

    /**
     * Traverses given array. If {@code pathCursor#getNext()} is
     * {@code null}, this method adds all the scalar values in current
     * array to the result. Otherwise, it traverses all objects in
     * given array and adds their scalar values named
     * {@code pathCursor#getNext()} to the result.
     *
     * Assumes the parser points to an array.
     *
     * @param parser
     * @param pathCursor
     * @return All matches in the current array that conform to
     * [any].lastPath search
     * @throws IOException
     */
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    private MultiResult getMultiValue(JsonParser parser,
                                      JsonPathCursor pathCursor) throws IOException {
        pathCursor.getNext();
        MultiResult<Object> multiResult = new MultiResult<>();

        JsonToken currentToken = parser.currentToken();
        if (currentToken != JsonToken.START_ARRAY) {
            return null;
        }
        while (true) {
            currentToken = parser.nextToken();
            if (currentToken == JsonToken.END_ARRAY) {
                break;
            }
            if (pathCursor.getCurrent() == null) {
                if (currentToken.isScalarValue()) {
                    multiResult.add(convertJsonTokenToValue(parser));
                } else {
                    parser.skipChildren();
                }
            } else {
                if (currentToken == START_OBJECT) {
                    do {
                        if (!findAttribute(parser, pathCursor, true)) {
                            break;
                        }

                        if ((parser.currentToken() != START_OBJECT && !pathCursor.hasNext())
                                || pathCursor.getNext() == null) {
                            addToMultiResult(multiResult, parser);
                            break;
                        }
                    } while (true);
                } else if (currentToken == JsonToken.START_ARRAY) {
                    parser.skipChildren();
                }
            }
        }
        return multiResult;
    }

    private static void addToMultiResult(MultiResult<Object> multiResult,
                                         JsonParser parser) throws IOException {
        if (parser.currentToken().isScalarValue()) {
            multiResult.add(convertJsonTokenToValue(parser));
        }
        while (parser.getCurrentToken() != JsonToken.END_OBJECT) {
            if (parser.currentToken().isStructStart()) {
                parser.skipChildren();
            }
            parser.nextToken();
        }
    }

    private static Object convertJsonTokenToValue(JsonParser parser) throws IOException {
        int token = parser.currentTokenId();
        switch (token) {
            case JsonTokenId.ID_STRING:
                return parser.getValueAsString();
            case JsonTokenId.ID_NUMBER_INT:
                return parser.getLongValue();
            case JsonTokenId.ID_NUMBER_FLOAT:
                return parser.getValueAsDouble();
            case JsonTokenId.ID_TRUE:
                return true;
            case JsonTokenId.ID_FALSE:
                return false;
            case JsonTokenId.ID_NULL:
                return null;
            default:
                return NonTerminalJsonValue.INSTANCE;
        }
    }
}
