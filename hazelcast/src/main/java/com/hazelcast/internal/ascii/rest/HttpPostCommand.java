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

package com.hazelcast.internal.ascii.rest;

import com.hazelcast.internal.ascii.NoOpCommand;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ascii.TextDecoder;
import com.hazelcast.util.StringUtil;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.HTTP_POST;
import static com.hazelcast.util.StringUtil.stringToBytes;

public class HttpPostCommand extends HttpCommand {

    private static final int RADIX = 16;
    @SuppressWarnings("checkstyle:magicnumber")
    private static final int INITIAL_CAPACITY = 1 << 8;
    // 65536, no specific reason, similar to UDP packet size limit
    @SuppressWarnings("checkstyle:magicnumber")
    private static final int MAX_CAPACITY = 1 << 16;
    private static final byte LINE_FEED = 0x0A;
    private static final byte CARRIAGE_RETURN = 0x0D;

    private final TextDecoder decoder;

    private boolean chunked;
    private boolean nextLine;
    private boolean readyToReadData;
    private ByteBuffer data;
    private String contentType;
    private ByteBuffer lineBuffer = ByteBuffer.allocate(INITIAL_CAPACITY);

    public HttpPostCommand(TextDecoder decoder, String uri) {
        super(HTTP_POST, uri);
        this.decoder = decoder;
    }

    /**
     * POST /path HTTP/1.0
     * User-Agent: HTTPTool/1.0
     * Content-TextCommandType: application/x-www-form-urlencoded
     * Content-Length: 45
     * <next_line>
     * <next_line>
     * byte[45]
     * <next_line>
     *
     * @param src
     * @return
     */
    @Override
    public boolean readFrom(ByteBuffer src) {
        boolean complete = doActualRead(src);
        while (!complete && readyToReadData && chunked && src.hasRemaining()) {
            complete = doActualRead(src);
        }
        if (complete) {
            if (data != null) {
                data.flip();
            }
        }
        return complete;
    }

    private boolean doActualRead(ByteBuffer cb) {
        if (readyToReadData) {
            if (chunked && (data == null || !data.hasRemaining())) {

                if (data != null && cb.hasRemaining()) {
                    readCRLFOrPositionChunkSize(cb);
                }

                boolean done = readChunkSize(cb);
                if (done) {
                    return true;
                }
            }

            IOUtil.copyToHeapBuffer(cb, data);
        }

        setReadyToReadData(cb);

        return !chunked && ((data != null) && !data.hasRemaining());
    }

    private void setReadyToReadData(ByteBuffer cb) {
        while (!readyToReadData && cb.hasRemaining()) {
            byte b = cb.get();
            if (b == CARRIAGE_RETURN) {
                readLF(cb);
                processLine(StringUtil.lowerCaseInternal(toStringAndClear(lineBuffer)));
                if (nextLine) {
                    readyToReadData = true;
                }
                nextLine = true;
                break;
            }

            nextLine = false;
            appendToBuffer(b);
        }
    }

    public byte[] getData() {
        if (data == null) {
            return null;
        } else {
            return data.array();
        }
    }

    byte[] getContentType() {
        if (contentType == null) {
            return null;
        } else {
            return stringToBytes(contentType);
        }
    }

    private void readCRLFOrPositionChunkSize(ByteBuffer cb) {
        byte b = cb.get();
        if (b == CARRIAGE_RETURN) {
            readLF(cb);
        } else {
            cb.position(cb.position() - 1);
        }
    }

    private void readLF(ByteBuffer cb) {
        assert cb.hasRemaining() : "'\\n' should follow '\\r'";

        byte b = cb.get();
        if (b != LINE_FEED) {
            throw new IllegalStateException("'\\n' should follow '\\r', but got '" + (char) b + "'");
        }
    }

    private String toStringAndClear(ByteBuffer bb) {
        if (bb == null) {
            return "";
        }
        String result;
        if (bb.position() == 0) {
            result = "";
        } else {
            result = StringUtil.bytesToString(bb.array(), 0, bb.position());
        }
        bb.clear();
        return result;
    }

    private boolean readChunkSize(ByteBuffer cb) {
        boolean hasLine = false;
        while (cb.hasRemaining()) {
            byte b = cb.get();
            if (b == CARRIAGE_RETURN) {
                readLF(cb);
                hasLine = true;
                break;
            }
            appendToBuffer(b);
        }

        if (hasLine) {
            String lineStr = toStringAndClear(lineBuffer).trim();

            // hex string
            int dataSize = lineStr.length() == 0 ? 0 : Integer.parseInt(lineStr, RADIX);
            if (dataSize == 0) {
                return true;
            }
            dataNullCheck(dataSize);
        }
        return false;
    }

    private void dataNullCheck(int dataSize) {
        if (data != null) {
            ByteBuffer newData = ByteBuffer.allocate(data.capacity() + dataSize);
            newData.put(data.array());
            data = newData;
        } else {
            data = ByteBuffer.allocate(dataSize);
        }
    }

    private void appendToBuffer(byte b) {
        if (!lineBuffer.hasRemaining()) {
            expandBuffer();
        }
        lineBuffer.put(b);
    }

    private void expandBuffer() {
        if (lineBuffer.capacity() == MAX_CAPACITY) {
            throw new BufferOverflowException();
        }

        int capacity = lineBuffer.capacity() << 1;

        ByteBuffer newBuffer = ByteBuffer.allocate(capacity);
        lineBuffer.flip();
        newBuffer.put(lineBuffer);
        lineBuffer = newBuffer;
    }

    private void processLine(String currentLine) {
        if (contentType == null && currentLine.startsWith(HEADER_CONTENT_TYPE)) {
            contentType = currentLine.substring(currentLine.indexOf(' ') + 1);
        } else if (data == null && currentLine.startsWith(HEADER_CONTENT_LENGTH)) {
            data = ByteBuffer.allocate(Integer.parseInt(currentLine.substring(currentLine.indexOf(' ') + 1)));
        } else if (!chunked && currentLine.startsWith(HEADER_CHUNKED)) {
            chunked = true;
        } else if (currentLine.startsWith(HEADER_EXPECT_100)) {
            decoder.sendResponse(new NoOpCommand(RES_100));
        }
    }
}
