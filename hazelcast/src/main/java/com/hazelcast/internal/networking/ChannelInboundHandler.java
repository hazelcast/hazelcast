/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking;

import java.nio.ByteBuffer;

/**
 * The {@link ChannelInboundHandler} provides control when data is received and needs to be processed. For example data
 * has received on the socket and needs to be decoded into a Packet.
 *
 * {@link ChannelInboundHandler} are not expected to be thread-safe; each channel will gets its own instance(s).
 *
 * A {@link ChannelInboundHandler} is constructed through a {@link ChannelInitializer}.
 *
 * If the main task of a ChannelInboundHandler is to decode a message (e.g. a Packet), it is best to call this handler a
 * decoder. For example PacketDecoder.
 *
 * @see ChannelOutboundHandler
 * @see EventLoopGroup
 * @see ChannelInitializer
 * @see ChannelErrorHandler
 * @see Channel
 */
public interface ChannelInboundHandler {

    /**
     * A callback to indicate that data is available in the src ByteBuffer to be processed.
     *
     * @param src the ByteBuffer containing the data to read. The ByteBuffer is already in reading mode and when completed,
     *            should not be converted to write-mode using clear/compact.
     * @throws Exception if something fails while reading data from the ByteBuffer or processing the data (e.g. when a Packet
     *                   fails to get processed). When an exception is thrown, the {@link ChannelErrorHandler} is called.
     */
    void onRead(ByteBuffer src) throws Exception;
}
