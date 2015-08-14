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

import com.hazelcast.nio.SocketWritable;


/**
 * Each {@link TcpIpConnection} has a WriteHandler. It reads data from the network into the system.
 */
public interface WriteHandler {

    /**
     * Returns the total number of packets (urgent and non normal priority) pending.
     *
     * @return total number of packets.
     */
    int totalPacketsPending();

    /**
     * Returns the last {@link com.hazelcast.util.Clock#currentTimeMillis()} a write completes.
     *
     * Completes means that it is written to the network buffers; not that it has been offered.
     *
     * @return the lst time a write completed.
     */
    long getLastWriteTime();

    /**
     * Offers a SocketWritable to be written.
     *
     * No guarantees are made that the packet is going to be written or received by the other side.
     *
     * @param packet the SocketWritable
     */
    void offer(SocketWritable packet);

    /**
     * Gets the {@link SocketWriter} that belongs to this WriteHandler.
     *
     * This method exists for the SocketTextReader, but probably should be deleted.
     *
     * @return the socket writer.
     */
    SocketWriter getSocketWriter();

    /**
     * Sets the protocol this WriteHandler should use.
     *
     * This should be called only once at the beginning of the connection.
     *
     * See {@link com.hazelcast.nio.Protocols}
     *
     * @param protocol the protocol
     */
    void setProtocol(String protocol);

    /**
     * Starts this WriteHandler.
     */
    void start();

    /**
     * Shuts down this WriteHandler.
     */
    void shutdown();
}
