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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.hazelcast.internal.json.JsonReducedValueParser;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Responsible for creating and using {@link JsonSchemaDescription}s.
 */
final class JsonSchemaHelper {

    private JsonSchemaHelper() {
        // private constructor
    }

    /**
     * Creates a pattern in the form of integer list. Created pattern
     * represents the logical position of the value pointed by {@code path}
     * in given input. For example;
     * A Json object is given:
     * {
     *     "attr1": 10,
     *     "attr2": [
     *          "aText",
     *          "anotherText"
     *     ]
     * }
     * The path "attr2[1]" represents "anotherText" JsonString. The logical
     * position of this value is "1.1" because "attr2" is the 2nd attribute
     * and we are looking for the second item in the corresponding array.
     *
     * @param input         a buffer containing the queried object.
     * @param offset        the offset of the beginning of the json
     *                      string within the input.
     * @param description   the description of the given object
     * @param path          query path
     * @return              a list of integers that represent the order
     *                      of each query term within the queried object
     * @throws IOException
     */
    @SuppressWarnings("checkstyle:npathcomplexity")
    static List<Integer> findPattern(BufferObjectDataInput input, int offset,
                                     JsonSchemaDescription description,
                                     String[] path) throws IOException {
        if (description instanceof JsonSchemaTerminalDescription) {
            if (isThisQuery(path)) {
                return Collections.EMPTY_LIST;
            } else {
                return null;
            }
        }
        List<Integer> pattern = new ArrayList<Integer>();
        for (int i = 0; i < path.length; i++) {
            if (!(description instanceof JsonSchemaStructDescription)) {
                return null;
            }
            if (isArrayPath(path[i])) {
                int index = getArrayIndex(path[i]);
                if (((JsonSchemaStructDescription) description).getChildCount() <= index) {
                    return null;
                }
                pattern.add(index);
                description = ((JsonSchemaStructDescription) description).getChild(index).getValue();
            } else {
                int numberOfElements = ((JsonSchemaStructDescription) description).getChildCount();
                boolean foundCurrent = false;

                // looking for the current path in object
                for (int j = 0; j < numberOfElements; j++) {
                    JsonSchemaNameValue nameValue = ((JsonSchemaStructDescription) description).getChild(j);
                    int nameAddress = nameValue.getNameStart();
                    input.position(nameAddress + offset);
                    if (!isAttributeName(path[i], input)) {
                        continue;
                    }
                    description = nameValue.getValue();
                    pattern.add(j);
                    foundCurrent = true;
                    break;
                }

                if (!foundCurrent) {
                    return null;
                }
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
     * @param input             a byte array containing the target object
     * @param offset            a pointer to the start of the target
     *                          object in input
     * @param description       valid schema description to the target
     *                          object. The behavior is undefined if
     *                          description does not match the actual
     *                          object
     * @param expectedPattern   see
     * {@link JsonSchemaHelper#findPattern(BufferObjectDataInput, int, JsonSchemaDescription, String[])}
     * @param attributePath
     * @return                  JsonValue extracted or null
     */
    static JsonValue findValueWithPattern(BufferObjectDataInput input, int offset,
                                                 JsonSchemaDescription description, List<Integer> expectedPattern,
                                                 String[] attributePath) throws IOException {
        if (expectedPattern.size() > 0) {
            for (int i = 0; i < expectedPattern.size(); i++) {
                if (!(description instanceof JsonSchemaStructDescription)) {
                    return null;
                }
                int attributeIndex = expectedPattern.get(i);
                JsonSchemaStructDescription nonLeafDescription = (JsonSchemaStructDescription) description;
                if (nonLeafDescription.getChildCount() <= attributeIndex) {
                    return null;
                }
                String currentPath = attributePath[i];
                JsonSchemaNameValue nameValue = nonLeafDescription.getChild(attributeIndex);
                if (!isStructMatch(input, offset, nameValue, attributeIndex, currentPath)) {
                    return null;
                }
                description = nameValue.getValue();
            }
        }
        if (expectedPattern.size() == 0 && isThisQuery(attributePath)
                || description instanceof JsonSchemaTerminalDescription) {
            input.position(((JsonSchemaTerminalDescription) description).getValueStartLocation() + offset);
            JsonReducedValueParser valueParser = new JsonReducedValueParser();
            return valueParser.parse(new InputStreamReader(new DataInputStream(input)));
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
    public static JsonSchemaDescription createDescription(JsonParser parser) throws IOException {
        JsonSchemaDescription dummy = new JsonSchemaStructDescription(null);
        JsonSchemaStructDescription parent = (JsonSchemaStructDescription) dummy;
        JsonToken currentToken = parser.nextToken();
        int nameLocation = -1;
        while (currentToken != null) {
            if (currentToken.isStructStart()) {
                JsonSchemaStructDescription nonLeaf = new JsonSchemaStructDescription(parent);
                JsonSchemaNameValue nameValue = new JsonSchemaNameValue(nameLocation, nonLeaf);
                parent.addChild(nameValue);
                parent = nonLeaf;
                nameLocation = -1;
            } else if (currentToken == JsonToken.FIELD_NAME) {
                nameLocation = (int) parser.getTokenLocation().getByteOffset();
            } else if (currentToken.isStructEnd()) {
                parent = parent.getParent();
                nameLocation = -1;
            } else {
                JsonSchemaTerminalDescription leaf = new JsonSchemaTerminalDescription(parent);
                leaf.setValueStartLocation((int) parser.getTokenLocation().getByteOffset());
                JsonSchemaNameValue nameValue = new JsonSchemaNameValue(nameLocation, leaf);
                parent.addChild(nameValue);
                nameLocation = -1;
            }
            currentToken = parser.nextToken();
        }
        JsonSchemaNameValue nameValue = ((JsonSchemaStructDescription) dummy).getChild(0);
        if (nameValue == null) {
            return null;
        }
        dummy = nameValue.getValue();
        dummy.setParent(null);
        return dummy;
    }

    private static boolean isAttributeName(String attributeName, ObjectDataInput is) throws IOException {
        byte[] nameBytes = attributeName.getBytes(Charset.forName("UTF8"));
        if (!isQuote(is)) {
            return false;
        }
        for (int i = 0; i < nameBytes.length; i++) {
            if (nameBytes[i] != is.readByte()) {
                return false;
            }
        }
        return isQuote(is);
    }

    private static boolean isStructMatch(BufferObjectDataInput input, int offset,
                                         JsonSchemaNameValue nameValue, int attributeIndex,
                                         String currentPath) throws IOException {
        int currentNamePos = nameValue.getNameStart();
        if (isArrayPath(currentPath)) {
            return currentNamePos == -1 && attributeIndex == getArrayIndex(currentPath);
        } else {
            if (currentNamePos == -1) {
                return false;
            }
            input.position(currentNamePos + offset);
            return isAttributeName(currentPath, input);
        }
    }

    private static boolean isQuote(ObjectDataInput is) throws IOException {
        return is.readByte() == '"';
    }

    private static boolean isThisQuery(String[] path) {
        return path.length == 1 && "this".equals(path[0]);
    }

    private static boolean isArrayPath(String p) {
        return p.endsWith("]");
    }

    private static int getArrayIndex(String p) {
        return Integer.parseInt(p.substring(0, p.length() - 1));
    }

    private static class DataInputStream extends InputStream {

        private BufferObjectDataInput input;

        DataInputStream(BufferObjectDataInput input) {
            this.input = input;
        }

        @Override
        public int read() throws IOException {
            try {
                return input.readByte();
            } catch (EOFException e) {
                return -1;
            }
        }
    }
}
