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

package com.hazelcast.spi.exception;

/**
 * Exception to indicate any issues while executing a REST call.
 */
public class RestClientException
        extends RuntimeException {
    private int httpErrorCode;

    public RestClientException(String message, int httpErrorCode) {
        super(String.format("%s. HTTP Error Code: %s", message, httpErrorCode));
        this.httpErrorCode = httpErrorCode;
    }

    public RestClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public int getHttpErrorCode() {
        return httpErrorCode;
    }
}
