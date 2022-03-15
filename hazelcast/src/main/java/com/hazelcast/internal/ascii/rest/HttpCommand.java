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

package com.hazelcast.internal.ascii.rest;

import com.hazelcast.internal.ascii.AbstractTextCommand;
import com.hazelcast.internal.ascii.TextCommandConstants;
import com.hazelcast.internal.ascii.TextCommandService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nullable;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_200;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_204;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_400;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_403;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_404;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_500;
import static com.hazelcast.internal.ascii.rest.HttpStatusCode.SC_503;
import static com.hazelcast.internal.nio.IOUtil.copyToHeapBuffer;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;

@SuppressFBWarnings({"EI_EXPOSE_REP", "MS_MUTABLE_ARRAY", "MS_PKGPROTECT"})
public abstract class HttpCommand extends AbstractTextCommand {

    public static final byte[] CONTENT_TYPE_PLAIN_TEXT = stringToBytes("text/plain");
    public static final byte[] CONTENT_TYPE_JSON = stringToBytes("application/json");
    public static final byte[] CONTENT_TYPE_BINARY = stringToBytes("application/binary");

    static final String HEADER_CONTENT_TYPE = "content-type: ";
    static final String HEADER_CONTENT_LENGTH = "content-length: ";
    static final String HEADER_CHUNKED = "transfer-encoding: chunked";
    static final String HEADER_EXPECT_100 = "expect: 100";

    private static final String HEADER_CUSTOM_PREFIX = "Hazelcast-";
    private static final byte[] CONTENT_TYPE = stringToBytes("Content-Type: ");
    private static final byte[] CONTENT_LENGTH = stringToBytes("Content-Length: ");

    protected final String uri;
    protected ByteBuffer response;
    protected boolean nextLine;
    private final RestCallExecution executionDetails = new RestCallExecution();

    public HttpCommand(TextCommandConstants.TextCommandType type, String uri) {
        super(type);
        this.uri = uri;
        // the command line was parsed already, let's start with clear next line
        this.nextLine = true;
        executionDetails.setRequestPath(uri);
    }

    @Override
    public boolean shouldReply() {
        return true;
    }

    public String getURI() {
        return uri;
    }

    /**
     * Prepares a {@link HttpURLConnection#HTTP_NO_CONTENT} response.
     */
    public void send204() {
        setResponse(SC_204, null, null);
    }

    /**
     * Prepares a {@link HttpURLConnection#HTTP_BAD_REQUEST} response.
     */
    public void send400() {
        setResponse(SC_400, null, null);
    }

    /**
     * Prepares a {@link HttpURLConnection#HTTP_FORBIDDEN} response.
     */
    public void send403() {
        setResponse(SC_403, null, null);
    }

    /**
     * Prepares a {@link HttpURLConnection#HTTP_NOT_FOUND} response.
     */
    public void send404() {
        setResponse(SC_404, null, null);
    }

    /**
     * Prepares a {@link HttpURLConnection#HTTP_INTERNAL_ERROR} response.
     */
    public void send500() {
        setResponse(SC_500, null, null);
    }

    /**
     * Prepares a {@link HttpURLConnection#HTTP_UNAVAILABLE} response.
     */
    public void send503() {
        setResponse(SC_503, null, null);
    }

    /**
     * Prepares an empty {@link HttpURLConnection#HTTP_OK} response.
     */
    public void send200() {
        setResponse(SC_200, null, null);
    }

    /**
     * Prepares a HTTP response with no content and the provided status line and
     * response headers.
     *
     * @param statusCode the HTTP response status code
     * @param headers    the map of response headers
     */
    public void setResponse(HttpStatusCode statusCode,
                            @Nullable Map<String, Object> headers) {
        byte[] statusLine = statusCode.statusLine;
        int size = statusLine.length;
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
        response.put(statusLine);
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
        upcast(response).flip();
        setStatusCode(statusCode.code);
    }

    /**
     * Prepares a response with the provided status line, binary-encoded
     * content type and response.
     * The format of response:
     *       Status-Line (that includes CRLF itself);
     *       for each header in header do:
     *          header + CLRF
     *       done;
     *       CRLF;
     *       response-body;
     *
     * @param statusCode the HTTP response status code
     * @param headers    the map of response headers (In addition to these, we also
     *                   add the content-length header)
     * @param value       binary-encoded response content value
     */
    public void setResponseWithHeaders(HttpStatusCode statusCode,
                                       @Nullable Map<String, Object> headers,
                                       @Nullable byte[] value) {
        byte[] statusLine = statusCode.statusLine;
        int valueSize = (value == null) ? 0 : value.length;
        byte[] len = stringToBytes(String.valueOf(valueSize));
        int size = statusLine.length;

        size += CONTENT_LENGTH.length;
        size += len.length;
        size += TextCommandConstants.RETURN.length;
        if (headers != null) {
            for (Map.Entry<String, Object> entry : headers.entrySet()) {
                size += stringToBytes(entry.getKey() + ": ").length;
                size += stringToBytes(entry.getValue().toString()).length;
                size += TextCommandConstants.RETURN.length;
            }
        }
        size += TextCommandConstants.RETURN.length;
        size += valueSize;

        this.response = ByteBuffer.allocate(size);
        response.put(statusLine);
        response.put(CONTENT_LENGTH);
        response.put(len);
        response.put(TextCommandConstants.RETURN);
        if (headers != null) {
            for (Map.Entry<String, Object> entry : headers.entrySet()) {
                response.put(stringToBytes(entry.getKey() + ": "));
                response.put(stringToBytes(entry.getValue().toString()));
                response.put(TextCommandConstants.RETURN);
            }
        }
        response.put(TextCommandConstants.RETURN);

        if (value != null) {
            response.put(value);
        }
        upcast(response).flip();
        setStatusCode(statusCode.code);
    }


    /**
     * Prepares a response with the provided status line, binary-encoded
     * content type and response.
     *
     * @param statusCode  the HTTP response status code
     * @param contentType binary-encoded response content type
     * @param value       binary-encoded response content value
     */
    public void setResponse(HttpStatusCode statusCode,
                            @Nullable byte[] contentType,
                            @Nullable byte[] value) {
        byte[] statusLine = statusCode.statusLine;
        int valueSize = (value == null) ? 0 : value.length;
        byte[] len = stringToBytes(String.valueOf(valueSize));
        int size = statusLine.length;
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
        response.put(statusLine);
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
        upcast(response).flip();
        setStatusCode(statusCode.code);
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

    public RestCallExecution getExecutionDetails() {
        return executionDetails;
    }

    void setStatusCode(int statusCode) {
        int existingStatusCode = executionDetails.getStatusCode();
        if (existingStatusCode > 0) {
            throw new IllegalStateException("can not set statusCode to " + statusCode + ", it is already " + existingStatusCode);
        }
        executionDetails.setStatusCode(statusCode);
    }

    @Override
    public void beforeSendResponse(TextCommandService textCommandService) {
        RestCallCollector collector = textCommandService.getRestCallCollector();
        collector.collectExecution(this);
    }
}
