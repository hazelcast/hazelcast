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

import com.hazelcast.nio.OutboundFrame;

/**
 * Each {@link TcpIpConnection} has a {@link SocketWriter} and it writes {@link OutboundFrame} instances to the socket. Copying
 * the Frame instances to the byte-buffer is done using the {@link WriteHandler}.
 *
 * Each {@link TcpIpConnection} has its own {@link SocketWriter} instance.
 *
 * Before Hazelcast 3.6 the name of this interface was WriteHandler.
 *
 * @see SocketReader
 * @see ReadHandler
 * @see IOThreadingModel
 */
public interface SocketWriter {

    /**
     * Returns the total number of packets (urgent and non normal priority) pending to be written to the socket.
     *
     * @return total number of pending packets.
     */
    int totalFramesPending();

    /**
     * Returns the last {@link com.hazelcast.util.Clock#currentTimeMillis()} that a write to the socket completed.
     *
     * Writing to the socket doesn't mean that data has been send or received; it means that data was written to the
     * SocketChannel. It could very well be that this data is stuck somewhere in an io-buffer.
     *
     * @return the last time something was written to the socket.
     */
    long getLastWriteTimeMillis();

    /**
     * Offers a Frame to be written to the socket.
     *
     * No guarantees are made that the frame is going to be written or received by the other side.
     *
     * todo: the name offer is misleading since it doesn't return a boolean.
     *
     * @param frame the Frame to write.
     */
    void offer(OutboundFrame frame);

    /**
     * Gets the {@link WriteHandler} that belongs to this SocketWriter.
     *
     * This method exists for the {@link com.hazelcast.nio.ascii.TextReadHandler}, but probably should be deleted.
     *
     * @return the WriteHandler
     */
    WriteHandler getWriteHandler();

    /**
     * Sets the protocol this SocketWriter should use.
     *
     * This should be called only once at the beginning of the connection.
     *
     * See {@link com.hazelcast.nio.Protocols}
     *
     * @param protocol the protocol
     */
    void setProtocol(String protocol);

    /**
     * Starts this SocketWriter.
     *
     * This method can be called from an arbitrary thread.
     *
     * @see #shutdown()
     */
    void start();

    /**
     * Shuts down this SocketWriter.
     *
     * This method can be called from an arbitrary thread.
     *
     * @see #start()
     */
    void shutdown();
}
