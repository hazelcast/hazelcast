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

package com.hazelcast.nio.tcp;

import com.hazelcast.util.counters.Counter;

/**
 * Each {@link TcpIpConnection} has a ReadHandler and it is responsible to read data from the socket on behalf of that
 * connection.
 */
public interface ReadHandler {

    /**
     * Returns the last {@link com.hazelcast.util.Clock#currentTimeMillis()} a read of network was done.
     *
     * @return the last time a read from the network was done.
     */
    long getLastReadTimeMillis();

    /**
     * Gets the Counter that counts the number of normal packets that have been read.
     *
     * @return the normal packets counter.
     */
    Counter getNormalPacketsReadCounter();

    /**
     * Gets the Counter that counts the number of priority packets that have been read.
     *
     * @return the priority packets counter.
     */
    Counter getPriorityPacketsReadCounter();

    /**
     * Starts this ReadHandler.
     */
    void start();

    /**
     * Shutdown this ReadHandler.
     */
    void shutdown();
}
