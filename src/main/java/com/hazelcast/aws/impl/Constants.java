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

package com.hazelcast.aws.impl;

public final class Constants {

    public static final String DATE_FORMAT = "yyyyMMdd'T'HHmmss'Z'";
    public static final String DOC_VERSION = "2016-11-15";
    public static final String SIGNATURE_METHOD_V4 = "AWS4-HMAC-SHA256";
    public static final String GET = "GET";
    public static final String ECS_CREDENTIALS_ENV_VAR_NAME = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";

    private Constants() {
    }
}
