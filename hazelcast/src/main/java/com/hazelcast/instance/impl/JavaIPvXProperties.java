/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl;

/**
 * These JVM system properties are only checked once at startup and should be
 * treated as immutable in our application.
 *
 * See https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/net/doc-files/net-properties.html#ipv4-ipv6-heading
 */
public record JavaIPvXProperties(boolean preferIPv4Stack, boolean preferIPv6Addresses) {

    private static final String PREFER_IPV4_STACK = "java.net.preferIPv4Stack";
    private static final String PREFER_IPV6_ADDRESSES = "java.net.preferIPv6Addresses";

    /**
     * The instance loaded from the real values of the system properties
     */
    public static final JavaIPvXProperties INSTANCE = new JavaIPvXProperties(Boolean.getBoolean(PREFER_IPV4_STACK),
            Boolean.getBoolean(PREFER_IPV6_ADDRESSES));
}
