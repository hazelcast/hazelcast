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


/**
 * ChannelHandler is either responsible for processing inbound or outbound data.
 *
 * ChannelHandlers are not expected to be thread-safe.
 *
 * @param <S> the type of the source. E.g. a ByteBuffer or
 *            a {@link java.util.function.Supplier}.
 * @param <D> the type of the destination. E.g. a ByteBuffer
 *            or a {@link java.util.function.Consumer}.
 */
public abstract class ChannelHandler<H extends ChannelHandler, S, D> {

    /**
     * The Channel this ChannelHandler handles. Only the owning pipeline
     * should  modify this field.
     */
    protected Channel channel;

    protected S src;
    protected D dst;

    /**
     * Gets the source of this ChannelHandler.
     *
     * This method should only be called from the thread that owns this
     * ChannelHandler.
     *
     * @return the source; could be null.
     */
    public S src() {
        return src;
    }

    /**
     * Sets the source of this ChannelHandler.
     *
     * This method should only be called from the thread that owns this
     * ChannelHandler.
     *
     * @param src the new source; is allowed to be null.
     */
    public void src(S src) {
        this.src = src;
    }

    /**
     * Gets the destination of this ChannelHandler.
     *
     * This method should only be called from the thread that owns this
     * ChannelHandler.
     *
     * @return the destination; could be null.
     */
    public D dst() {
        return dst;
    }

    /**
     * Sets the destination of this ChannelHandler.
     *
     * This method should only be called from the thread that owns this
     * ChannelHandler.
     *
     * @param dst the new destination; is allowed to be null.
     */
    public void dst(D dst) {
        this.dst = dst;
    }

    /**
     * Sets the Channel.
     *
     * Should only be called by the {@link InboundPipeline}.
     *
     * @param channel the Channel
     * @return this instance
     */
    public final H setChannel(Channel channel) {
        this.channel = channel;
        return (H) this;
    }

    /**
     * Gets called when this ChannelHandler is added to the pipeline.
     *
     * This method should execute very quickly because it could be executed on
     * an io thread.
     *
     * Will be called from a thread owning this handler.
     */
    public void handlerAdded() {
    }

    /**
     * Intercepts an error that is thrown while processing the inbound or
     * outbound pipeline.
     *
     * The default implementation doesn't do anything and the original
     * {@link Throwable} is send to the {@link ChannelErrorHandler}.
     *
     * However, in some cases like with TLS a more informative exception
     * needs to be thrown. To prevent losing the stacktrace, it is best to
     * include the original Throwable as cause in the new Throwable.
     *
     * This method will only be called from the thread owning the pipeline
     * this handler is part of.
     *
     * No modifications should be done to the pipeline and no attempts
     * should be made to 'repair' the channel. When a Throwable is thrown,
     * it is the end of the channel.
     *
     * @param error the Throwable thrown while processing the pipeline.
     * @throws Throwable the new Throwable
     */
    public void interceptError(Throwable error) throws Throwable {
    }
}
