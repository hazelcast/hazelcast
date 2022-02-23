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

package com.hazelcast.internal.serialization.impl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.hazelcast.internal.json.JsonReducedValueParser;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.query.impl.getters.JsonPathCursor;

import java.io.IOException;

/**
 * An adapter offering utility methods over a data input containing
 * Json content.
 */
public abstract class NavigableJsonInputAdapter {

    /**
     * Sets the position of underlying data input.
     * @param position
     */
    public abstract void position(int position);

    /**
     * Gives the current position of the cursor.
     *
     * @return the current read position of the cursor
     */
    public abstract int position();

    /**
     * Resets the read cursor.
     */
    public abstract void reset();

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
     * @param cursor
     * @return {@code true} if the given attributeName matches the one
     *          in underlying data input
     */
    public abstract boolean isAttributeName(JsonPathCursor cursor);

    /**
     * Tries to parse a single JsonValue from the input. The {@code offset}
     * must be pointing to the beginning of a scalar Json value.
     *
     * @param parser
     * @param offset
     * @return either null or a scalar JsonValue.
     * @throws IOException if offset is pointing to a non-scalar
     *                      or invalid Json input
     */
    public abstract JsonValue parseValue(JsonReducedValueParser parser, int offset) throws IOException;

    /**
     * Creates a parser from given factory
     *
     * @param factory
     * @return the parser
     * @throws IOException
     */
    public abstract JsonParser createParser(JsonFactory factory) throws IOException;
}
