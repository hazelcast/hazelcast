/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl.ascii.rest;

import com.hazelcast.impl.ascii.AbstractTextCommand;
import com.hazelcast.nio.IOUtil;

import java.nio.ByteBuffer;

public abstract class HttpCommand extends AbstractTextCommand {
    public static final String HEADER_CONTENT_TYPE = "content-type: ";
    public static final String HEADER_CONTENT_LENGTH = "content-length: ";
    public static final String HEADER_CHUNKED = "transfer-encoding: chunked";
    public static final String HEADER_EXPECT_100 = "expect: 100";

    protected final String uri;
    protected ByteBuffer response;

    public static final byte[] RES_200 = "HTTP/1.1 200 OK\r\n".getBytes();
    public static final byte[] RES_400 = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n".getBytes();
    public static final byte[] RES_403 = "HTTP/1.1 403 Forbidden\r\n\r\n".getBytes();
    public static final byte[] RES_404 = "HTTP/1.1 404 Not Found\r\n\r\n".getBytes();
    public static final byte[] RES_100 = "HTTP/1.1 100 Continue\r\n\r\n".getBytes();
    public static final byte[] RES_204 = "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n".getBytes();
    public static final byte[] RES_503 = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n".getBytes();
    public static final byte[] RES_500 = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n".getBytes();
    public static final byte[] CONTENT_TYPE = "Content-Type: ".getBytes();
    public static final byte[] CONTENT_LENGTH = "Content-Length: ".getBytes();
    public static final byte[] CONTENT_TYPE_PLAIN_TEXT = "text/plain".getBytes();
    public static final byte[] CONTENT_TYPE_BINARY = "application/binary".getBytes();

    public HttpCommand(TextCommandType type, String uri) {
        super(type);
        this.uri = uri;
    }

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
//    public boolean writeTo(ByteBuffer bb) {
//        while (bb.hasRemaining() && response.hasRemaining()) {
//            bb.put(response.get());
//        }
//        return !response.hasRemaining();
//    }

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
        byte[] len = String.valueOf(valueSize).getBytes();
        int size = RES_200.length;
        if (contentType != null) {
            size += CONTENT_TYPE.length;
            size += contentType.length;
            size += RETURN.length;
        }
        size += CONTENT_LENGTH.length;
        size += len.length;
        size += RETURN.length;
        size += RETURN.length;
        size += valueSize;
        size += RETURN.length;
        this.response = ByteBuffer.allocate(size);
        response.put(RES_200);
        if (contentType != null) {
            response.put(CONTENT_TYPE);
            response.put(contentType);
            response.put(RETURN);
        }
        response.put(CONTENT_LENGTH);
        response.put(len);
        response.put(RETURN);
        response.put(RETURN);
        if (value != null) {
            response.put(value);
        }
        response.put(RETURN);
        response.flip();
    }

    public boolean writeTo(ByteBuffer bb) {
        IOUtil.copyToHeapBuffer(response, bb);
        return !response.hasRemaining();
    }

    @Override
    public String toString() {
        return "HttpCommand [" + type + "]{" +
                "uri='" + uri + '\'' +
                '}' + super.toString();
    }
}