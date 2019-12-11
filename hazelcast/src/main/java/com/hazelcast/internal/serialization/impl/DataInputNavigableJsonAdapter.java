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
import com.hazelcast.spi.impl.operationexecutor.impl.OperationThread;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

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

    static class UTF8Reader extends Reader {

        static final ThreadLocal<CharsetDecoder> DECODER_THREAD_LOCAL = ThreadLocal.withInitial(Bits.UTF_8::newDecoder);

        private final ByteBuffer inputBuffer;
        // required to support read() for multibyte characters
        private boolean hasLeftoverChar;
        private int leftoverChar;

        UTF8Reader(BufferObjectDataInput input) {
            byte[] data = obtainBytes(input);
            inputBuffer = ByteBuffer.wrap(data);
            inputBuffer.position(input.position());
        }

        // default read() implementation does not handle multibyte chars well
        @Override
        public int read() throws IOException {
            if (hasLeftoverChar) {
                hasLeftoverChar = false;
                return leftoverChar;
            }

            char[] buffer = new char[2];
            int charsRead = read(buffer, 0, 2);
            switch (charsRead) {
                case -1:
                    return -1;
                case 2:
                    leftoverChar = buffer[1];
                    hasLeftoverChar = true;
                    return buffer[0];
                case 1:
                    return buffer[0];
                default:
                    throw new IllegalStateException("Unexpected result from read: " + charsRead);
            }
        }

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            final CharsetDecoder decoder = Thread.currentThread() instanceof OperationThread
                    ? DECODER_THREAD_LOCAL.get()
                    : Bits.UTF_8.newDecoder();
            decoder.reset();
            if (off < 0 || (off + len) > cbuf.length) {
                throw new IndexOutOfBoundsException();
            }
            if (!inputBuffer.hasRemaining()) {
                return -1;
            }
            CharBuffer charbuffer = CharBuffer.wrap(cbuf, off, len);
            CoderResult result = decoder.decode(inputBuffer, charbuffer, true);
            if (result.isError()) {
                result.throwException();
            }
            if (result.isUnderflow()) {
                // return number of characters read
                if (!inputBuffer.hasRemaining()) {
                    decoder.flush(charbuffer);
                }
                return charbuffer.position() - off;
            } else {
                // result is overflow:
                // the given cbuf did not fit all characters, inputBuffer still has bytes remaining
                return charbuffer.position() - off;
            }
        }

        private byte[] obtainBytes(BufferObjectDataInput input) {
            if (input instanceof ByteArrayObjectDataInput) {
                return ((ByteArrayObjectDataInput) input).data;
            } else {
                throw new IllegalArgumentException("All BufferObjectDataInput are instances of ByteArrayObjectDataInput");
            }
        }
        @Override
        public void close() throws IOException {

        }
    }
}
