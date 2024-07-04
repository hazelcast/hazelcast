/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import java.net.HttpURLConnection;

import static com.hazelcast.internal.util.StringUtil.stringToBytes;

public enum HttpStatusCode {

    SC_100(100, "Continue"),
    SC_200(HttpURLConnection.HTTP_OK, "OK"),
    SC_204(HttpURLConnection.HTTP_NO_CONTENT, "No Content"),
    SC_400(HttpURLConnection.HTTP_BAD_REQUEST, "Bad Request"),
    SC_403(HttpURLConnection.HTTP_FORBIDDEN, "Forbidden"),
    SC_404(HttpURLConnection.HTTP_NOT_FOUND, "Not Found"),
    SC_500(HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Server Error"),
    SC_503(HttpURLConnection.HTTP_UNAVAILABLE, "Service Unavailable")
    ;

    final int code;
    final byte[] statusLine;

    @SuppressWarnings("squid:S3457")
    HttpStatusCode(int code, String statusLine) {
        this.code = code;
        this.statusLine = stringToBytes(String.format("HTTP/1.1 %d %s\r\n", code, statusLine));
    }
}
