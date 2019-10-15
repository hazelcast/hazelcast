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
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.query.impl.getters.JsonPathCursor;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;

public class DataInputNavigableJsonAdapter extends NavigableJsonInputAdapter {

    private final int initialOffset;
    private BufferObjectDataInput input;

    public DataInputNavigableJsonAdapter(BufferObjectDataInput input, int initialOffset) {
        this.input = input;
        this.input.position(initialOffset);
        this.initialOffset = initialOffset;
    }

    @Override
    public void position(int position) {
        input.position(position + initialOffset);
    }

    @Override
    public int position() {
        return input.position() - initialOffset;
    }

    @Override
    public void reset() {
        input.position(initialOffset);
    }

    @Override
    public boolean isAttributeName(JsonPathCursor cursor) {
        try {
            byte[] nameBytes = cursor.getCurrentAsUTF8();
            if (!isQuote()) {
                return false;
            }
            for (int i = 0; i < nameBytes.length; i++) {
                if (nameBytes[i] != input.readByte()) {
                    return false;
                }
            }
            return isQuote();
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public JsonValue parseValue(JsonReducedValueParser parser, int offset) throws IOException {
        input.position(offset + initialOffset);
        return parser.parse(new UTF8Reader(input));
    }

    @Override
    public JsonParser createParser(JsonFactory factory) throws IOException {
        return factory.createParser(SerializationUtil.convertToInputStream(input, initialOffset));
    }

    private boolean isQuote() throws IOException {
        return input.readByte() == '"';
    }

    private static class UTF8Reader extends Reader {

        private final DataInput input;

        UTF8Reader(DataInput input) {
            this.input = input;
        }

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            if (off < 0 || (off + len) > cbuf.length) {
                throw new IndexOutOfBoundsException();
            }
            int i = 0;
            try {
                for (i = 0; i < len; i++) {
                    byte firstByte = input.readByte();
                    char c = Bits.readUtf8Char(input, firstByte);
                    cbuf[off + i] = c;
                }
            } catch (EOFException e) {
                if (i == 0) {
                    return -1;
                }
            }
            return i;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
