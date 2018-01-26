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

import com.hazelcast.nio.tcp.MemberChannelInboundHandler;

import java.nio.ByteBuffer;

/**
 * The {@link ChannelInboundHandler} provides control when data is received and needs to be processed. For example data
 * has received on the socket and needs to be decoded into a Packet.
 *
 * {@link ChannelInboundHandler} are not expected to be thread-safe; each channel will gets its own instance(s).
 *
 * A {@link ChannelInboundHandler} is constructed through a {@link ChannelInitializer}.
 *
 * <h1>Pipelining & Encryption</h1>
 * The ChannelInboundHandler/ChannelOutboundHandler can also form a pipeline. For example for SSL there could be a initial
 * ChannelInboundHandler that decryption the ByteBuffer and passes the decrypted ByteBuffer to the next ChannelInboundHandler;
 * which could be a {@link MemberChannelInboundHandler} that reads out any Packet from the decrypted ByteBuffer. Using this
 * approach encryption can easily be added to any type of communication, not only member 2 member communication.
 *
 * Currently security is added by using a {@link Channel}, but this is not needed if the handlers form a pipeline.
 * Netty follows a similar approach with pipelining and adding encryption.
 *
 * There is no explicit support for setting up a 'pipeline' of ChannelInboundHandler/WriterHandlers but t can easily be realized
 * by setting up the chain and let a handler explicitly forward to the next. Since it isn't a common practice for the handler
 * so far, isn't needed to add additional complexity to the system; just set up a chain manually.
 *
 * pseudo code:
 * <pre>
 *     public class DecryptingReadHandler implements ChannelInboundHandler {
 *         private final ChannelInboundHandler next;
 *
 *         public DecryptingReadHandler(ChannelInboundHandler next) {
 *             this.next = next;
 *         }
 *
 *         public void onRead(ByteBuffer src) {
 *             decrypt(src, decryptedSrc);
 *             next.onRead(decryptedSrc)
 *         }
 *     }
 * </pre>
 * The <code>next</code> ChannelInboundHandler is the next item in the pipeline.
 *
 * For encryption is similar approach can be followed where the DecryptingWriteHandler is the last ChannelOutboundHandler in
 * the pipeline.
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
