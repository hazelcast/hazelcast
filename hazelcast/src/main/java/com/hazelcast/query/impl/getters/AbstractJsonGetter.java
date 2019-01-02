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
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.io.IOException;

public abstract class AbstractJsonGetter extends Getter {

    private static final String DELIMITER = "\\.|\\[";

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
        throw new HazelcastSerializationException("Unknown Json type: " + value);
    }

    @Override
    Object getValue(Object obj) {
        throw new HazelcastException("Path agnostic value extraction is not supported");

    }

    @Override
    Object getValue(Object obj, String attributePath) throws Exception {
        String[] paths = getPath(attributePath);
        JsonParser parser = createParser(obj);

        try {
            parser.nextToken();
            for (int i = 0; i < paths.length; i++) {
                String path = paths[i];
                String arrayIndexText = getIndexTextOrNull(path);
                if (arrayIndexText == null) {
                    // non array case
                    if (!findAttribute(parser, path)) {
                        return null;
                    }
                } else {
                    // array case
                    if ("any".equals(arrayIndexText)) {
                        String lastPath = i + 1 < paths.length ? paths[i + 1] : null;
                        return getMultiValue(parser, lastPath);
                    } else {
                        JsonToken token = parser.currentToken();
                        if (token != JsonToken.START_ARRAY) {
                            return null;
                        }
                        token = parser.nextToken();
                        int arrayIndex = Integer.parseInt(arrayIndexText);
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

    @Override
    Class getReturnType() {
        throw new IllegalArgumentException("Non applicable for Json getters");
    }

    @Override
    boolean isCacheable() {
        return false;
    }

    abstract JsonParser createParser(Object obj) throws IOException;

    /**
     * Looks for the attribute with the given name only in current object. If found, parser points to the value
     * of the given attribute when this method returns. If given {@code path} does not exist in the current level,
     * then parser points to matching {@code JsonToken.END_OBJECT} of the current object.
     *
     * Assumes the parser points to a {@code JsonToken.START_OBJECT}
     * @param parser
     * @param path
     * @return {@code true} if given attribute name exists in the current object
     * @throws IOException
     */
    private boolean findAttribute(JsonParser parser, String path) throws IOException {
        JsonToken token = parser.getCurrentToken();
        if (token != JsonToken.START_OBJECT) {
            return false;
        }
        while (true) {
            token = parser.nextToken();
            if (token == JsonToken.END_OBJECT) {
                return false;
            }
            if (path.equals(parser.getCurrentName())) {
                parser.nextToken();
                return true;
            } else {
                parser.nextToken();
                parser.skipChildren();
            }
        }
    }

    /**
     * Traverses given array. If {@code lastPath} is {@code null}, this
     * method adds all the scalar values in current array to the result.
     * Otherwise, it traverses all objects in given array and adds their
     * scalar values named {@code lastPath} to the result.
     *
     * Assumes the parser points to an array.
     *
     * @param parser
     * @param lastPath
     * @return All matches in the current array that conform to
     *          [any].lastPath search
     * @throws IOException
     */
    private MultiResult getMultiValue(JsonParser parser, String lastPath) throws IOException {
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
            if (lastPath == null) {
                if (currentToken.isScalarValue()) {
                    multiResult.add(convertJsonTokenToValue(parser));
                } else {
                    parser.skipChildren();
                }
            } else {
                if (currentToken == JsonToken.START_OBJECT && findAttribute(parser, lastPath)) {
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

    /**
     * Extracts path name from a query if query is in the form of [pathName].*
     * @param path
     * @return path name of {@code null}
     */
    private String getIndexTextOrNull(String path) {
        if (path.charAt(path.length() - 1) == ']') {
            return path.substring(0, path.length() - 1);
        } else {
            return null;
        }
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

    private String[] getPath(String attributePath) {
        return attributePath.split(DELIMITER);
    }
}
