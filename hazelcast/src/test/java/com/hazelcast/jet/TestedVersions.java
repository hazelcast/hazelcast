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
package com.hazelcast.jet;

import org.testcontainers.utility.DockerImageName;

import static org.testcontainers.utility.DockerImageName.parse;

/**
 * Simple placeholder for versions of various Docker images.
 */
public final class TestedVersions {
    public static final String MONGO_VERSION = System.getProperty("test.mongo.version", "7.0.5");
    public static final String TOXIPROXY_VERSION = System.getProperty("test.toxiproxy.version", "2.7.0");
    public static final DockerImageName TOXIPROXY_IMAGE = parse("ghcr.io/shopify/toxiproxy:" + TOXIPROXY_VERSION)
            .asCompatibleSubstituteFor("shopify/toxiproxy");

    public static final String DEFAULT_ORACLE_IMAGE_NAME = "gvenzl/oracle-xe:21-slim-faststart";
    public static final String ORACLE_PROPERTY_NAME = "test.oracle.version";
    public static final String TEST_ORACLE_VERSION = System.getProperty(ORACLE_PROPERTY_NAME, DEFAULT_ORACLE_IMAGE_NAME);

    public static final String TEST_AZURE_SQL_EDGE_VERSION = System.getProperty("test.azuresqledge.version", "1.0.7");

    public static final String TEST_MSSQLSERVER_VERSION = System.getProperty("test.mssqlserver.version", "2022-latest");
}
