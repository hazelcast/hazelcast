/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.data.io;

import com.hazelcast.jet.impl.actor.RingBufferActor;
import com.hazelcast.nio.Address;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Represents abstract task to read from network socket;
 *
 * The architecture is following:
 *
 *
 * <pre>
 *  SocketChannel (JetAddress)  -&gt; SocketReader -&gt; Consumer(ringBuffer)
 * </pre>
 */
public interface SocketReader extends NetworkTask {
    /**
     * @return - true of last write to the consumer-queue has been flushed;
     */
    boolean isFlushed();

    /**
     * Assign corresponding socketChannel to reader-task;
     *
     * @param socketChannel  - network socketChannel;
     * @param receiveBuffer  - byteBuffer to be used to read data from socket;
     * @param isBufferActive - true , if buffer can be used for read (not all data has been read), false otherwise
     */
    void setSocketChannel(SocketChannel socketChannel,
                          ByteBuffer receiveBuffer, boolean isBufferActive);

    /**
     * Register output ringBuffer consumer;
     *
     * @param ringBufferActor - corresponding output consumer;
     */
    void registerConsumer(RingBufferActor ringBufferActor);

    /**
     * Assigns corresponding socket writer on the opposite node;
     *
     * @param writeAddress - JET's member node address;
     * @param socketWriter - SocketWriter task;
     */
    void assignWriter(Address writeAddress,
                      SocketWriter socketWriter);
}
