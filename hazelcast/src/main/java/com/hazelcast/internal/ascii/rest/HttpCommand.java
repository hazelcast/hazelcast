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

import com.hazelcast.internal.ascii.AbstractTextCommand;
import com.hazelcast.internal.ascii.TextCommandConstants;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.hazelcast.nio.IOUtil.copyToHeapBuffer;
import static com.hazelcast.util.StringUtil.stringToBytes;

@SuppressFBWarnings({"EI_EXPOSE_REP", "MS_MUTABLE_ARRAY", "MS_PKGPROTECT"})
public abstract class HttpCommand extends AbstractTextCommand {

    public static final String HEADER_CONTENT_TYPE = "content-type: ";
    public static final String HEADER_CONTENT_LENGTH = "content-length: ";
    public static final String HEADER_CHUNKED = "transfer-encoding: chunked";
    public static final String HEADER_EXPECT_100 = "expect: 100";
    public static final String HEADER_CUSTOM_PREFIX = "Hazelcast-";
    public static final byte[] RES_200 = stringToBytes("HTTP/1.1 200 OK\r\n");
    public static final byte[] RES_400 = stringToBytes("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n");
    public static final byte[] RES_403 = stringToBytes("HTTP/1.1 403 Forbidden\r\n\r\n");
    public static final byte[] RES_404 = stringToBytes("HTTP/1.1 404 Not Found\r\n\r\n");
    public static final byte[] RES_100 = stringToBytes("HTTP/1.1 100 Continue\r\n\r\n");
    public static final byte[] RES_204 = stringToBytes("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n");
    public static final byte[] RES_503 = stringToBytes("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n");
    public static final byte[] RES_500 = stringToBytes("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
    public static final byte[] CONTENT_TYPE = stringToBytes("Content-Type: ");
    public static final byte[] CONTENT_LENGTH = stringToBytes("Content-Length: ");
    public static final byte[] CONTENT_TYPE_PLAIN_TEXT = stringToBytes("text/plain");
    public static final byte[] CONTENT_TYPE_JSON = stringToBytes("application/javascript");
    public static final byte[] CONTENT_TYPE_BINARY = stringToBytes("application/binary");

    protected final String uri;
    protected ByteBuffer response;
    protected boolean nextLine;


    public HttpCommand(TextCommandConstants.TextCommandType type, String uri) {
        super(type);
        this.uri = uri;
    }

    @Override
    public boolean shouldReply() {
        return true;
    }

    public String getURI() {
        return uri;
    }

    public void send204() {
        this.response = ByteBuffer.wrap(RES_204);
    }

    public void send400() {
        this.response = ByteBuffer.wrap(RES_400);
    }

    public void setResponse(byte[] value) {
        this.response = ByteBuffer.wrap(value);
    }

    public void send200() {
        setResponse(null, null);
    }


    /**
     * HTTP/1.0 200 OK
     * Content-Length: 0
     * Custom-Header1: val1
     * Custom-Header2: val2
     *
     * @param headers
     */
    public void setResponse(Map<String, Object> headers) {
        int size = RES_200.length;
        byte[] len = stringToBytes(String.valueOf(0));
        size += CONTENT_LENGTH.length;
        size += len.length;
        size += TextCommandConstants.RETURN.length;
        if (headers != null) {
            for (Map.Entry<String, Object> entry : headers.entrySet()) {
                size += stringToBytes(HEADER_CUSTOM_PREFIX + entry.getKey() + ": ").length;
                size += stringToBytes(entry.getValue().toString()).length;
                size += TextCommandConstants.RETURN.length;
            }
        }
        size += TextCommandConstants.RETURN.length;
        this.response = ByteBuffer.allocate(size);
        response.put(RES_200);
        response.put(CONTENT_LENGTH);
        response.put(len);
        response.put(TextCommandConstants.RETURN);
        if (headers != null) {
            for (Map.Entry<String, Object> entry : headers.entrySet()) {
                response.put(stringToBytes(HEADER_CUSTOM_PREFIX + entry.getKey() + ": "));
                response.put(stringToBytes(entry.getValue().toString()));
                response.put(TextCommandConstants.RETURN);
            }
        }
        response.put(TextCommandConstants.RETURN);
        response.flip();
    }

    /**
     * HTTP/1.0 200 OK
     * Date: Fri, 31 Dec 1999 23:59:59 GMT
     * Content-TextCommandType: text/html
     * Content-Length: 1354
     *
     * @param contentType
     * @param value
     */
    public void setResponse(byte[] contentType, byte[] value) {
        int valueSize = (value == null) ? 0 : value.length;
        byte[] len = stringToBytes(String.valueOf(valueSize));
        int size = RES_200.length;
        if (contentType != null) {
            size += CONTENT_TYPE.length;
            size += contentType.length;
            size += TextCommandConstants.RETURN.length;
        }
        size += CONTENT_LENGTH.length;
        size += len.length;
        size += TextCommandConstants.RETURN.length;
        size += TextCommandConstants.RETURN.length;
        size += valueSize;
        this.response = ByteBuffer.allocate(size);
        response.put(RES_200);
        if (contentType != null) {
            response.put(CONTENT_TYPE);
            response.put(contentType);
            response.put(TextCommandConstants.RETURN);
        }
        response.put(CONTENT_LENGTH);
        response.put(len);
        response.put(TextCommandConstants.RETURN);
        response.put(TextCommandConstants.RETURN);
        if (value != null) {
            response.put(value);
        }
        response.flip();
    }

    @Override
    public boolean writeTo(ByteBuffer dst) {
        copyToHeapBuffer(response, dst);
        return !response.hasRemaining();
    }

    @Override
    public boolean readFrom(ByteBuffer src) {
        while (src.hasRemaining()) {
            char c = (char) src.get();
            if (c == '\n') {
                if (nextLine) {
                    return true;
                }
                nextLine = true;
            } else if (c != '\r') {
                nextLine = false;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "HttpCommand ["
                + type + "]{"
                + "uri='"
                + uri
                + '\''
                + '}'
                + super.toString();
    }
}
