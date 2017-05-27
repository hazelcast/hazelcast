/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 * Responsible for writing {@link OutboundFrame} to a {@link ByteBuffer}. For example a Packet needs to be written to a socket,
 * then it is taken from the queue of pending packets, the ChannelOutboundHandler is called with the Packet and the socket buffer
 * as argument and will then write the content of the packet to the buffer. And on completion, the content of the buffer is
 * written to the socket.
 *
 * {@link ChannelOutboundHandler} are not expected to be thread-safe; each channel will gets its own instance(s).
 *
 * A {@link ChannelOutboundHandler} is constructed through a {@link ChannelInitializer}.
 *
 * For more information about the ChannelOutboundHandler (and handlers in generally), have a look at the
 * {@link ChannelInboundHandler}.
 *
 * @param <F>
 * @see EventLoopGroup
 * @see ChannelInboundHandler
 * @see ChannelInitializer
 * @see ChannelErrorHandler
 * @see Channel
 */
public interface ChannelOutboundHandler<F extends OutboundFrame> {

    /**
     * A callback to indicate that the Frame should be written to the destination ByteBuffer.
     *
     * It could be that a Frame is too big to fit into the ByteBuffer in 1 go; in that case this call will be made
     * for the same Frame multiple times until write returns true.
     *
     * @param frame the Frame to write
     * @param dst   the destination ByteBuffer
     * @return true if the Frame is completely written
     * @throws Exception if something fails while writing to ByteBuffer. When an exception is thrown, the
     *                   {@link ChannelErrorHandler} is called.
     */
    boolean onWrite(F frame, ByteBuffer dst) throws Exception;
}
