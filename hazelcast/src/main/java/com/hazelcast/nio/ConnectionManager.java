/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

    /**
     * Starts ConnectionManager, initializes its resources, starts threads etc.
     * After start ConnectionManager becomes fully operational.
     * <p/>
     * If it's already started, then this method has no effect.
     *
     * @throws IllegalStateException if ConnectionManager is shutdown
     */
    void start();

    /**
     * Stops ConnectionManager, releases its resources, stops threads etc.
     * When stopped ConnectionManager can be started again using {@link #start()}.
     * <p/>
     * This method has no effect if it's already stopped or shutdown.
     * <p/>
     * Currently <tt>stop</tt> is called during merge process
     * to detach node from current cluster. After node becomes ready to join to
     * the new cluster, <tt>start</tt> is called to re-initialize the ConnectionManager.
     */
    void stop();

    /**
     * Shutdowns ConnectionManager completely. ConnectionManager won't be operational anymore and
     * cannot be restarted.
     * <p/>
     * This method has no effect if it's already shutdown.
     */
    void shutdown();

    void addConnectionListener(ConnectionListener connectionListener);
}
