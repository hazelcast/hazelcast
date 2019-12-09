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

package com.hazelcast.internal.serialization.impl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.hazelcast.internal.json.JsonReducedValueParser;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.query.impl.getters.JsonPathCursor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.StringReader;

/**
 * An adapter offering utility methods over a data input containing JSON content.
 */
public class NavigableJsonInputAdapter {
    private final int initialOffset;
    private String source;
    private int pos;

    public NavigableJsonInputAdapter(String source, int initialOffset) {
        this.initialOffset = initialOffset;
        this.source = source;
        this.pos = initialOffset;
    }

    /**
     * Sets the position of underlying data input.
     * @param position Position.
     */
    public void position(int position) {
        this.pos = initialOffset + position;
    }

    /**
     * Gives the current position of the cursor.
     *
     * @return the current read position of the cursor
     */
    public int position() {
        return pos - initialOffset;
    }

    /**
     * Resets the read cursor.
     */
    public void reset() {
        pos = initialOffset;
    }

    /**
     * This method verifies that the underlying input currently points
     * to an attribute name of a Json object. {@link JsonPathCursor#getCurrent()}
     * method is called to retrieve the attribute name.
     *
     * The cursor is moved to the end of the attribute name, after
     * quote character if successful. Otherwise cursor might be in
     * any position further forward than where it was prior to this
     * method.
     *
     * @param cursor Cursor.
     * @return {@code true} if the given attributeName matches the one
     *          in underlying data input
     */
    public boolean isAttributeName(JsonPathCursor cursor) {
        if (source.length() < pos + cursor.getCurrent().length() + 2) {
            return false;
        }
        if (source.charAt(pos++) != '"') {
            return false;
        }
        for (int i = 0; i < cursor.getCurrent().length() && pos < source.length(); i++) {
            if (cursor.getCurrent().charAt(i) != source.charAt(pos++)) {
                return false;
            }
        }
        return source.charAt(pos++) == '"';
    }

    /**
     * Tries to parse a single JsonValue from the input. The {@code offset}
     * must be pointing to the beginning of a scalar Json value.
     *
     * @param parser Parser.
     * @param offset Offset.
     * @return either null or a scalar JsonValue.
     * @throws IOException if offset is pointing to a non-scalar
     *                      or invalid Json input
     */
    @SuppressFBWarnings("SR_NOT_CHECKED")
    public JsonValue parseValue(JsonReducedValueParser parser, int offset) throws IOException {
        StringReader reader = new StringReader(source);
        if (reader.skip(initialOffset + offset) != initialOffset + offset) {
            throw new IOException("There are not enough characters in this string");
        }
        return parser.parse(reader);
    }

    /**
     * Creates a parser from given factory
     *
     * @param factory Factory.
     * @return the parser
     * @throws IOException If failed.
     */
    public JsonParser createParser(JsonFactory factory) throws IOException {
        return factory.createParser(new StringReader(source));
    }
}
