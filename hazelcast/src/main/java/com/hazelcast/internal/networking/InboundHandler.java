/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.nio.IOUtil.newByteBuffer;

/**
 * The {@link InboundHandler} provides control when data is received and
 * needs to be processed. For example data has received on the socket and needs
 * to be decoded into a Packet.
 * <p>
 * An {@link InboundHandler} is not expected to be thread-safe; each channel
 * will get its own instance(s).
 * <p>
 * An {@link InboundHandler} is constructed through a {@link ChannelInitializer}.
 * <p>
 * If the main task of a InboundHandler is to decode a message (e.g. a Packet),
 * it is best to call this handler a decoder. For example PacketDecoder.
 *
 * @see OutboundHandler
 * @see Networking
 * @see ChannelInitializer
 * @see ChannelErrorHandler
 * @see Channel
 */
public abstract class InboundHandler<S, D> extends ChannelHandler<InboundHandler, S, D> {

    /**
     * A callback to indicate that data is available in the src to be
     * processed.
     *
     * InboundHandlers should be able to deal with spurious onReads
     * (so a read even though there is nothing to be processed).
     *
     * @return HandlerStatus the status of the handler after processing the src.
     * @throws Exception if something fails while reading data from the src
     *                   or processing the data (e.g. when a Packet fails to get processed). When an
     *                   exception is thrown, the {@link ChannelErrorHandler} is called.
     */
    public abstract HandlerStatus onRead() throws Exception;

    /**
     * Initializes the src buffer. Should only be called by InboundHandler
     * implementations that have a ByteBuffer as source.
     *
     * The capacity of the src buffer will come from the {@link ChannelOptions} using
     * {@link ChannelOption#SO_RCVBUF}
     */
    protected final void initSrcBuffer() {
        initSrcBuffer(channel.options().getOption(SO_RCVBUF));
    }

    /**
     * Initializes the src buffer. Should only be called by InboundHandler
     * implementations that have a ByteBuffer as source.
     *
     * @param sizeBytes the size of the srcBuffer in bytes.
     */
    protected final void initSrcBuffer(int sizeBytes) {
        ChannelOptions config = channel.options();
        src = (S) newByteBuffer(sizeBytes, config.getOption(DIRECT_BUF));
    }
}
