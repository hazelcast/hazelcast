/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.aws;

/**
 * Thrown to indicate an error while connecting to AWS.
 * <p>
 * A list of error codes can be found at: {@see http://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html}.
 */
class AwsConnectionException
    extends RuntimeException {
    private final int httpReponseCode;
    private final String errorMessage;

    AwsConnectionException(int httpReponseCode, String errorMessage) {
        super(messageFrom(httpReponseCode, errorMessage));
        this.httpReponseCode = httpReponseCode;
        this.errorMessage = errorMessage;
    }

    private static String messageFrom(int httpReponseCode, String errorMessage) {
        return String.format("Connection to AWS failed (HTTP Response Code: %s, Message: \"%s\")", httpReponseCode, errorMessage);
    }

    int getHttpReponseCode() {
        return httpReponseCode;
    }

    String getErrorMessage() {
        return errorMessage;
    }
}
