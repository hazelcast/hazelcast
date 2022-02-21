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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.json.ReaderBasedJsonParser;
import com.fasterxml.jackson.core.json.UTF8StreamJsonParser;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.json.JsonReducedValueParser;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.json.NonTerminalJsonValue;
import com.hazelcast.internal.json.ParseException;
import com.hazelcast.internal.serialization.impl.NavigableJsonInputAdapter;
import com.hazelcast.query.impl.getters.JsonPathCursor;

import java.io.IOException;

/**
 * Responsible for creating and using {@link JsonSchemaNode}s.
 */
public final class JsonSchemaHelper {

    private JsonSchemaHelper() {
        // private constructor
    }

    /**
     * Creates a {@link JsonPattern} for given query path. If the path
     * matches a terminal value in the schema, then the schema is
     * returned. Otherwise this method returns null. If given path
     * has "any", pattern is created only upto that part. The rest is
     * omitted.
     *
     * @param input         a buffer containing the queried object.
     * @param schemaNode the description of the given object
     * @param path          query path
     * @return              a pattern object matching the path or null
     *                      when path does not match a terminal value
     * @throws IOException
     */
    @SuppressWarnings("checkstyle:npathcomplexity")
    public static JsonPattern createPattern(NavigableJsonInputAdapter input, JsonSchemaNode schemaNode, JsonPathCursor path) {
        if (schemaNode.isTerminal()) {
            return null;
        }
        JsonPattern pattern = new JsonPattern();
        while (path.getNext() != null) {
            if (schemaNode.isTerminal()) {
                return null;
            }
            int suggestedIndexInPath;
            int numberOfChildren = ((JsonSchemaStructNode) schemaNode).getChildCount();
            if (path.isArray()) {
                if (path.isAny()) {
                    pattern.addAny();
                    return pattern;
                }
                suggestedIndexInPath = path.getArrayIndex();
            } else {
                for (suggestedIndexInPath = 0; suggestedIndexInPath < numberOfChildren; suggestedIndexInPath++) {
                    JsonSchemaNameValue nameValue = ((JsonSchemaStructNode) schemaNode)
                            .getChild(suggestedIndexInPath);
                    int nameAddress = nameValue.getNameStart();
                    if (nameAddress < 0) {
                        return null;
                    }
                    input.position(nameAddress);
                    if (input.isAttributeName(path)) {
                        break;
                    }
                }
            }
            if (isValidIndex(suggestedIndexInPath, (JsonSchemaStructNode) schemaNode, path.isArray())) {
                schemaNode = ((JsonSchemaStructNode) schemaNode).getChild(suggestedIndexInPath).getValue();
                pattern.add(suggestedIndexInPath);
            } else {
                return null;
            }
        }
        return pattern;
    }

    /**
     * Extract the JsonValue that is stored in attributePath in input.
     * This method validates expectedPattern using attributePath while
     * extraction. If expectedPattern points to invalid or wrong attributes
     * at any point, this method returns {@code null}
     *
     * NOTE: this method cannot handle patterns with "any" in it.
     *
     * @param input             a byte array containing the target object
     * @param schemaNode        valid schema description to the target
     *                          object. The behavior is undefined if
     *                          description does not match the actual
     *                          object
     * @param expectedPattern   this cannot contain "any"
     * @param attributePath     this cannot contain "any"
     * @return                  JsonValue extracted or null
     */
    @SuppressWarnings("checkstyle:npathcomplexity")
    public static JsonValue findValueWithPattern(NavigableJsonInputAdapter input,
                                                 JsonSchemaNode schemaNode, JsonPattern expectedPattern,
                                                 JsonPathCursor attributePath) throws IOException {
        for (int i = 0; i < expectedPattern.depth(); i++) {
            if (attributePath.getNext() == null) {
                return null;
            }
            if (schemaNode.isTerminal()) {
                return null;
            }
            int expectedOrderIndex = expectedPattern.get(i);
            JsonSchemaStructNode structDescription = (JsonSchemaStructNode) schemaNode;
            if (structDescription.getChildCount() <= expectedOrderIndex) {
                return null;
            }
            JsonSchemaNameValue nameValue = structDescription.getChild(expectedOrderIndex);
            if (structMatches(input, nameValue, expectedOrderIndex, attributePath)) {
                schemaNode = nameValue.getValue();
            } else {
                return null;
            }
        }
        if (attributePath.getNext() == null) {
            if (schemaNode.isTerminal()) {
                // at this point we are sure we found the value by pattern. So we have to be able to extract JsonValue.
                // Otherwise, let the exceptions propagate
                try {
                    JsonReducedValueParser valueParser = new JsonReducedValueParser();
                    int valuePos = ((JsonSchemaTerminalNode) schemaNode).getValueStartLocation();
                    return input.parseValue(valueParser, valuePos);
                } catch (ParseException parseException) {
                    throw new HazelcastException(parseException);
                }
            } else {
                return NonTerminalJsonValue.INSTANCE;
            }
        }
        return null;
    }

    /**
     * Creates a description out of a JsonValue. The parser must be
     * pointing to the start of the input.
     *
     * @param parser
     * @return
     * @throws IOException
     */
    public static JsonSchemaNode createSchema(JsonParser parser) throws IOException {
        JsonSchemaNode dummy = new JsonSchemaStructNode(null);
        JsonSchemaStructNode parent = (JsonSchemaStructNode) dummy;
        JsonToken currentToken = parser.nextToken();
        int nameLocation = -1;
        if (currentToken == null) {
            return null;
        }
        while (currentToken != null) {
            if (currentToken.isStructStart()) {
                JsonSchemaStructNode structNode = new JsonSchemaStructNode(parent);
                JsonSchemaNameValue nameValue = new JsonSchemaNameValue(nameLocation, structNode);
                parent.addChild(nameValue);
                parent = structNode;
                nameLocation = -1;
            } else if (currentToken == JsonToken.FIELD_NAME) {
                nameLocation = (int) getTokenLocation(parser);
            } else if (currentToken.isStructEnd()) {
                parent = parent.getParent();
                nameLocation = -1;
            } else {
                JsonSchemaTerminalNode terminalNode = new JsonSchemaTerminalNode(parent);
                terminalNode.setValueStartLocation((int) getTokenLocation(parser));
                JsonSchemaNameValue nameValue = new JsonSchemaNameValue(nameLocation, terminalNode);
                parent.addChild(nameValue);
                nameLocation = -1;
            }
            currentToken = parser.nextToken();
        }
        JsonSchemaNameValue nameValue = ((JsonSchemaStructNode) dummy).getChild(0);
        if (nameValue == null) {
            return null;
        }
        dummy = nameValue.getValue();
        dummy.setParent(null);
        return dummy;
    }

    private static boolean isValidIndex(int suggestedIndex, JsonSchemaStructNode structNode, boolean isArrayPath) {
        if (suggestedIndex >= structNode.getChildCount()) {
            return false;
        }
        JsonSchemaNameValue nameValue = structNode.getChild(suggestedIndex);
        return (nameValue.isArrayItem() && isArrayPath) || (nameValue.isObjectItem() && !isArrayPath);
    }

    private static long getTokenLocation(JsonParser parser) {
        if (parser instanceof ReaderBasedJsonParser) {
            return parser.getTokenLocation().getCharOffset();
        } else if (parser instanceof UTF8StreamJsonParser) {
            return parser.getTokenLocation().getByteOffset();
        } else {
            throw new HazelcastException("Provided parser does not support location: "
                    + parser.getClass().getName());
        }
    }

    private static boolean structMatches(NavigableJsonInputAdapter input,
                                         JsonSchemaNameValue nameValue, int attributeIndex, JsonPathCursor currentPath) {
        int currentNamePos = nameValue.getNameStart();
        if (currentPath.isArray()) {
            return currentNamePos == -1 && attributeIndex == currentPath.getArrayIndex();
        } else {
            if (currentNamePos == -1) {
                return false;
            }
            input.position(currentNamePos);
            return input.isAttributeName(currentPath);
        }
    }
}
