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

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.nio.IOUtil.newByteBuffer;

/**
 * The {@link ChannelOutboundHandler} is a {@link ChannelHandler} for outbound
 * traffic.
 *
 * An example is the PacketEncoder that takes packets from the src (Provider) and
 * encodes them to the dst (ByteBuffer).
 *
 * {@link ChannelOutboundHandler} instances are not expected to be thread-safe;
 * each channel will gets its own instance(s).
 *
 * A {@link ChannelOutboundHandler} is constructed through a {@link ChannelInitializer}.
 *
 * <h1>Buffer</h1>
 * The ChannelOutboundHandler is responsible for its own destination buffer
 * if it has one. So if needs to be compacted/flipped etc, it should take
 * care of that.
 *
 * If ChannelOutboundHandler has an destination buffer and the {@link #onWrite()}
 * is called, the first thing it should do is to call
 * {@link com.hazelcast.nio.IOUtil#compactOrClear(ByteBuffer)} so it flips to
 * writing mode. And at the end of the onWrite method, the destination buffer
 * should be flipped into reading mode.
 *
 * If the ChannelOutboundHandler has a source buffer, it is expected to be
 * in reading mode and it is the responsibility of the ChannelOutboundHandler
 * in front to put that buffer in reading mode.
 *
 * @param <S> the type of the source. E.g. a ByteBuffer or a
 *            {@link com.hazelcast.util.function.Supplier}.
 * @param <D> the type of the destination. E.g. a ByteBuffer or a
 *            {@link com.hazelcast.util.function.Consumer}.
 * @see EventLoopGroup
 * @see ChannelInboundHandler
 * @see ChannelInitializer
 * @see ChannelErrorHandler
 * @see Channel
 */
public abstract class ChannelOutboundHandler<S, D> extends ChannelHandler<ChannelOutboundHandler, S, D> {

    /**
     * A callback to indicate that this ChannelOutboundHandler should be
     * processed.
     *
     * A ChannelOutboundHandler should be able to deal with a spurious wakeup.
     * So it could be for example there is no frame for it to write to.
     *
     * @return true if the content is fully written and this handler is clean.
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
        initDstBuffer(channel.config().getOption(SO_SNDBUF));
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

        ChannelConfig config = channel.config();
        ByteBuffer buffer = newByteBuffer(sizeBytes, config.getOption(DIRECT_BUF));
        if (bytes != null) {
            buffer.put(bytes);
        }
        buffer.flip();
        dst = (D) buffer;
    }
}
