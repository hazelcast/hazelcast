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

import com.hazelcast.nio.OutboundFrame;

import java.nio.ByteBuffer;

/**
 * Responsible for writing {@link OutboundFrame} to a {@link ByteBuffer}.
 *
 * {@link ChannelOutboundHandler} don't need tobe thread-safe; each channel should gets private instances.
 *
 * For more information about the ChannelOutboundHandler (and handlers in generally), have a look at the
 * {@link ChannelInboundHandler}.
 *
 * @param <F>
 * @see EventLoopGroup
 */
public interface ChannelOutboundHandler<F extends OutboundFrame> {

    /**
     * A callback to indicate that the Frame should be written to the destination ByteBuffer.
     *
     * It could be that a Frame is too big to fit into the ByteBuffer in 1 go; in that case this call will be made
     * for the same Frame multiple times until write returns true. It is up to the Frame to track where
     * it needs to continue.
     *
     * @param frame the Frame to write
     * @param dst   the destination ByteBuffer
     * @return true if the Frame is completely written
     * @throws Exception if something fails while writing to ByteBuffer. When an exception is thrown, the TcpIpConnection is
     *                   closed. There is no point continuing with a potentially corrupted stream.
     */
    boolean onWrite(F frame, ByteBuffer dst) throws Exception;
}
