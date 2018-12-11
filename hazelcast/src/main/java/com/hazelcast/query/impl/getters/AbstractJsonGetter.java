/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

    public AbstractJsonGetter(Getter parent) {
        super(parent);
    }

    public static Object convertFromJsonValue(JsonValue value) {
        if (value == null) {
            return null;
        } else if (value.isNumber()) {
            try {
                return value.asLong();
            } catch (NumberFormatException e) {
                return value.asDouble();
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

    protected String[] getPath(String attributePath) {
        return attributePath.split(DELIMITER);
    }

    @Override
    Object getValue(Object obj) throws Exception {
        throw new HazelcastException("Path agnostic value extraction is not supported");

    }

    @Override
    Object getValue(Object obj, String attributePath) throws Exception {
        String[] paths = getPath(attributePath);
        JsonParser parser = createParser(obj);

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
                    JsonToken token = parser.getCurrentToken();
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
        Object ret = convertJsonTokenToValue(parser);
        parser.close();
        return ret;
    }

    abstract JsonParser createParser(Object obj) throws IOException;

    private boolean findAttribute(JsonParser parser, String path) throws IOException {
        JsonToken token = parser.getCurrentToken();
        if (token != JsonToken.START_OBJECT) {
            return false;
        }
        parser.getCurrentToken();
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
            if (currentToken.isScalarValue()) {
                multiResult.add(convertJsonTokenToValue(parser));
            } else {
                if (lastPath != null && findAttribute(parser, lastPath)) {
                    multiResult.add(convertJsonTokenToValue(parser));
                }
                while (parser.getCurrentToken() != JsonToken.END_OBJECT) {
                    if (parser.currentToken().isStructStart()) {
                        parser.skipChildren();
                    }
                    parser.nextToken();
                }
            }
            parser.skipChildren();
        }
        return multiResult;
    }

    @Override
    Class getReturnType() {
        return null;
    }

    @Override
    boolean isCacheable() {
        return false;
    }

    private String getIndexTextOrNull(String path) {
        if (path.charAt(path.length() - 1) == ']') {
            return path.substring(0, path.length() - 1);
        } else {
            return null;
        }
    }

    private Object convertJsonTokenToValue(JsonParser parser) throws IOException {
        int token = parser.getCurrentTokenId();
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
}
