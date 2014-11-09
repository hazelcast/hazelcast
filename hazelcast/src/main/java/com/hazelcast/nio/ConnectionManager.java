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

package com.hazelcast.nio;

/**
 * Responsible for managing {@link com.hazelcast.nio.Connection} objects.
 */
public interface ConnectionManager {

    int getCurrentClientConnections();

    int getConnectionCount();

    int getAllTextConnections();

    int getActiveConnectionCount();

    Connection getConnection(Address address);

    Connection getOrConnect(Address address);

    Connection getOrConnect(Address address, boolean silent);

    boolean registerConnection(Address address, Connection connection);

    void destroyConnection(Connection conn);

    void shutdown();

    void start();

    void restart();

    void addConnectionListener(ConnectionListener connectionListener);

    /**
     * Adds all kinds of performance metrics. It is up to the implementation to add anything.
     */
    void dumpPerformanceMetrics(StringBuffer sb);
}
