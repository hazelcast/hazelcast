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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.StringReader;

public class StringNavigableJsonAdapter extends NavigableJsonInputAdapter {

    private final int initialOffset;
    private String source;
    private int pos;

    public StringNavigableJsonAdapter(String source, int initialOffset) {
        this.initialOffset = initialOffset;
        this.source = source;
        this.pos = initialOffset;
    }

    @Override
    public void position(int position) {
        this.pos = initialOffset + position;
    }

    @Override
    public int position() {
        return pos - initialOffset;
    }

    @Override
    public void reset() {
        pos = initialOffset;
    }

    @Override
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

    @SuppressFBWarnings("SR_NOT_CHECKED")
    @Override
    public JsonValue parseValue(JsonReducedValueParser parser, int offset) throws IOException {
        StringReader reader = new StringReader(source);
        if (reader.skip(initialOffset + offset) != initialOffset + offset) {
            throw new IOException("There are not enough characters in this string");
        }
        return parser.parse(reader);
    }

    @Override
    public JsonParser createParser(JsonFactory factory) throws IOException {
        return factory.createParser(new StringReader(source));
    }
}
