/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dataconnection.impl.jdbcproperties;

/**
 * The constants that can be used in HZ connections
 * These need to be translated to a Connection Pool implementation
 */
public final class DataConnectionProperties {

    public static final String JDBC_URL = "jdbcUrl";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String CONNECTION_TIMEOUT = "connectionTimeout";
    public static final String IDLE_TIMEOUT = "idleTimeout";
    public static final String KEEP_ALIVE_TIME = "keepaliveTime";
    public static final String MAX_LIFETIME = "maxLifetime";
    public static final String MINIMUM_IDLE = "minimumIdle";
    public static final String MAXIMUM_POOL_SIZE = "maximumPoolSize";

    private DataConnectionProperties() {
    }
}
