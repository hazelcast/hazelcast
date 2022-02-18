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

import com.hazelcast.internal.nio.IOUtil;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.nio.IOUtil.newByteBuffer;
import static com.hazelcast.internal.util.JVMUtil.upcast;

/**
 * The {@link OutboundHandler} is a {@link ChannelHandler} for outbound
 * traffic.
 *
 * An example is the PacketEncoder that takes packets from the src (Provider) and
 * encodes them to the dst (ByteBuffer).
 *
 * An {@link OutboundHandler} instances are not expected to be thread-safe;
 * each channel will get its own instance(s).
 *
 * An {@link OutboundHandler} is constructed through a {@link ChannelInitializer}.
 *
 * <h1>Buffer</h1>
 * The OutboundHandler is responsible for its own destination buffer
 * if it has one. So if the buffer needs to be compacted/flipped etc., it should
 * take care of that.
 *
 * If OutboundHandler has a destination buffer and the {@link #onWrite()}
 * is called, the first thing it should do is to call
 * {@link IOUtil#compactOrClear(ByteBuffer)} so it flips to
 * writing mode. And at the end of the onWrite method, the destination buffer
 * should be flipped into reading mode.
 *
 * If the OutboundHandler has a source buffer, it is expected to be
 * in reading mode, and it is the responsibility of the OutboundHandler
 * in front to put that buffer in reading mode.
 *
 * @param <S> the type of the source. E.g. a ByteBuffer or a
 *            {@link java.util.function.Supplier}.
 * @param <D> the type of the destination. E.g. a ByteBuffer or a
 *            {@link java.util.function.Consumer}.
 * @see Networking
 * @see InboundHandler
 * @see ChannelInitializer
 * @see ChannelErrorHandler
 * @see Channel
 */
public abstract class OutboundHandler<S, D> extends ChannelHandler<OutboundHandler, S, D> {

    /**
     * A callback to indicate that this OutboundHandler should be
     * processed.
     *
     * A OutboundHandler should be able to deal with a spurious wakeup.
     * So it could be for example there is no frame for it to write to.
     *
     * @return {@link HandlerStatus} the status of the handler after processing the src.
     * @throws Exception if something fails while executing the onWrite. When
     *                   an exception is thrown, the {@link ChannelErrorHandler}
     *                   is called.
     */
    public abstract HandlerStatus onWrite() throws Exception;

    /**
     * Initializes the dst ByteBuffer with the value for {@link ChannelOption#SO_SNDBUF}.
     *
     * The buffer created is reading mode.
     */
    protected final void initDstBuffer() {
        initDstBuffer(channel.options().getOption(SO_SNDBUF));
    }

    /**
     * Initializes the dst ByteBuffer with the configured size.
     *
     * The buffer created is reading mode.
     *
     * @param sizeBytes the size of the dst ByteBuffer.
     */
    protected final void initDstBuffer(int sizeBytes) {
        initDstBuffer(sizeBytes, null);
    }

    /**
     * Initializes the dst ByteBuffer with the configured size.
     *
     * The buffer created is reading mode.
     *
     * @param sizeBytes the size of the dst ByteBuffer.
     * @param bytes     the bytes added to the buffer. Can be null if nothing
     *                  should be added.
     * @throws IllegalArgumentException if the size of the buffer is too small.
     */
    protected final void initDstBuffer(int sizeBytes, byte[] bytes) {
        if (bytes != null && bytes.length > sizeBytes) {
            throw new IllegalArgumentException("Buffer overflow. Can't initialize dstBuffer for "
                    + this + " and channel" + channel + " because too many bytes, sizeBytes " + sizeBytes
                    + ". bytes.length " + bytes.length);
        }

        ChannelOptions config = channel.options();
        ByteBuffer buffer = newByteBuffer(sizeBytes, config.getOption(DIRECT_BUF));
        if (bytes != null) {
            buffer.put(bytes);
        }
        upcast(buffer).flip();
        dst = (D) buffer;
    }
}
