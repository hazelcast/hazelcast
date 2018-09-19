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

package com.hazelcast.internal.util.rest;

import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.rest.HttpCommand;
import com.hazelcast.internal.ascii.rest.HttpPostCommand;
import com.hazelcast.util.StringUtil;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.StringUtil.stringToBytes;

/**
 * Utility methods for REST calls.
 */

public final class RestUtil {

    private RestUtil() {
    }

    public static String exceptionResponse(Throwable throwable) {
        return response(ResponseType.FAIL, "message", throwable.getMessage());
    }

    public static String response(ResponseType type, String... attributes) {
        final StringBuilder builder = new StringBuilder("{");
        builder.append("\"status\":\"").append(type).append("\"");
        if (attributes.length > 0) {
            for (int i = 0; i < attributes.length; ) {
                final String key = attributes[i++];
                final String value = attributes[i++];
                if (value != null) {
                    builder.append(String.format(",\"%s\":\"%s\"", key, value));
                }
            }
        }
        return builder.append("}").toString();
    }

    /**
     * Decodes HTTP post params contained in {@link HttpPostCommand#getData()}. The data
     * should be encoded in UTF-8 and joined together with an ampersand (&).
     *
     * @param command    the HTTP post command
     * @param paramCount the number of parameters expected in the command
     * @return the decoded params
     * @throws UnsupportedEncodingException If character encoding needs to be consulted, but
     *                                      named character encoding is not supported
     */
    public static String[] decodeParams(HttpPostCommand command, int paramCount) throws UnsupportedEncodingException {
        final byte[] data = command.getData();
        final String[] encoded = bytesToString(data).split("&");
        final String[] decoded = new String[encoded.length];
        for (int i = 0; i < paramCount; i++) {
            decoded[i] = URLDecoder.decode(encoded[i], "UTF-8");
        }
        return decoded;
    }

    public static void sendResponse(TextCommandService textCommandService, HttpCommand command, String value) {
        command.setResponse(HttpCommand.CONTENT_TYPE_JSON, stringToBytes(value));
        textCommandService.sendResponse(command);
    }

    public enum ResponseType {
        SUCCESS, FAIL, FORBIDDEN;

        @Override
        public String toString() {
            return super.toString().toLowerCase(StringUtil.LOCALE_INTERNAL);
        }
    }

}

