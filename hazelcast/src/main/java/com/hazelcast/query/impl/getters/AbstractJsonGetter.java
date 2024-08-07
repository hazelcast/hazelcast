/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
    public Object getValue(Object obj, String attributePath) {
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
                    if (!findAttribute(parser, pathCursor)) {
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
    Class<?> getReturnType() {
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
     * <p>
     * Assumes the parser points to a {@code JsonToken.START_OBJECT}
     *
     * @return {@code true} if given attribute name exists in the current object
     * @throws IOException
     */
    private boolean findAttribute(JsonParser parser, JsonPathCursor pathCursor) throws IOException {
        JsonToken token = parser.getCurrentToken();
        if (token != START_OBJECT) {
            return false;
        }
        while (true) {
            token = parser.nextToken();
            if (token == JsonToken.END_OBJECT) {
                return false;
            }
            if (token == JsonToken.FIELD_NAME && pathCursor.getCurrent().equals(parser.getCurrentName())) {
                // current token matched, advance to next token before returning
                parser.nextToken();
                return true;
            } else if (token.isStructStart()) {
                // we want to match attributes, skip nested structures
                // (in particular the ones that are values of other attributes)
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
     * <p>
     * Assumes the parser points to an array.
     *
     * @return All matches in the current array that conform to
     * [any].lastPath search
     * @throws IOException
     */
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    private MultiResult getMultiValue(JsonParser parser,
                                      JsonPathCursor pathCursor) throws IOException {
        // consume [any] from cursor
        pathCursor.getNext();
        // stack for cursor state and backtracking
        int initialState = pathCursor.saveState();
        MultiResult<Object> multiResult = new MultiResult<>();

        JsonToken currentToken = parser.currentToken();
        if (currentToken != JsonToken.START_ARRAY) {
            return null;
        }
        while ((currentToken = parser.nextToken()) != JsonToken.END_ARRAY) {
            if (currentToken == null) {
                // EOF, probably malformed JSON. Ignore as we may already have some results.
                break;
            }

            // here we are always on the level of the original array
            if (pathCursor.getCurrent() == null) {
                // We want scalar values directly from the array.
                // Note that this is an exception to the rule: predicate that ends with array like `someArray[any]`
                // skips non-scalars instead or returning NonTerminalJsonValue.
                if (currentToken.isScalarValue()) {
                    multiResult.add(convertJsonTokenToValue(parser));
                } else {
                    parser.skipChildren();
                }
            } else {
                // we want attribute of an object contained in the array
                // nested arrays currently are not supported in paths and ignored in JSON.
                if (currentToken == START_OBJECT) {
                    boolean found = true;
                    // this can be thought of as number of START_OBJECT tokens consumed
                    // without consuming matching END_OBJECT because we are still in the middle of the object.
                    int subpathElementsMatched = 0;
                    do {
                        if (pathCursor.isArray()) {
                            throw new UnsupportedOperationException("Nested arrays in JSON paths are not supported");
                        }

                        if (!findAttribute(parser, pathCursor)) {
                            // if not found, findAttribute ends at the end of the object, but it may be a nested object
                            found = false;
                            break;
                        }

                        // we are one level deeper if we found an attribute
                        subpathElementsMatched++;
                    } while (pathCursor.getNext() != null);

                    if (found) {
                        // Be consistent with single value (getValue) - return NonTerminalJsonValue if applicable.
                        // See comment above about scalars directly from the array.
                        multiResult.add(convertJsonTokenToValue(parser));
                        if (!parser.currentToken().isScalarValue()) {
                            // skip current object which was returned as NonTerminalJsonValue
                            parser.skipChildren();
                        }
                    }

                    // jump out of current outer array element to be ready for next iteration.
                    // even if we found path, we do not try to find it again in the same object - duplicates are ignored
                    for (int unnestCounter = 0; unnestCounter < subpathElementsMatched; ++unnestCounter) {
                        advanceToTheEndOfCurrentObject(parser);
                    }

                    // Restore pathCursor for next search.
                    // Note that previous attempt might have ended early.
                    pathCursor.restoreState(initialState);
                } else if (currentToken == JsonToken.START_ARRAY) {
                    if (pathCursor.isArray()) {
                        // user-friendly error
                        throw new UnsupportedOperationException("Nested arrays in JSON paths are not supported");
                    }
                    parser.skipChildren();
                }
            }
        }

        return multiResult;
    }

    private static void advanceToTheEndOfCurrentObject(JsonParser parser) throws IOException {
        JsonToken currentToken;
        while ((currentToken = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (currentToken == null) {
                // EOF, probably malformed JSON. Ignore as we may already have some results.
                break;
            }
            if (currentToken.isStructStart()) {
                parser.skipChildren();
            }
        }
    }

    private static Object convertJsonTokenToValue(JsonParser parser) throws IOException {
        int token = parser.currentTokenId();
        return switch (token) {
            case JsonTokenId.ID_STRING -> parser.getValueAsString();
            case JsonTokenId.ID_NUMBER_INT -> parser.getLongValue();
            case JsonTokenId.ID_NUMBER_FLOAT -> parser.getValueAsDouble();
            case JsonTokenId.ID_TRUE -> true;
            case JsonTokenId.ID_FALSE -> false;
            case JsonTokenId.ID_NULL -> null;
            default -> NonTerminalJsonValue.INSTANCE;
        };
    }
}
