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
 * The outbound pipeline of a {@link Channel}. So all data that gets
 * written to the network, goes through the outbound pipeline.
 */
public interface OutboundPipeline {

    /**
     * Adds the handlers at the end of the pipeline
     *
     * No verification is done if the handler is already added and a handler
     * should only be added once.
     *
     * This method should only be made on the thread 'owning' the pipeline.
     *
     * @param handlers the handlers to add.
     * @return this
     */
    OutboundPipeline addLast(OutboundHandler... handlers);

    /**
     * Replaces the old OutboundHandler by the new ones. So if there
     * is a sequence of handlers [H1,H2,H3] and H2 gets replaced by [H4,H5]
     * the new pipeline will be [H1,H4,H5,H3].
     *
     * No verification is done if any of the handlers is already added and a
     * handler should only be added once.
     *
     * This method should only be made on the thread 'owning' the pipeline.
     *
     * @param oldHandler  the handlers to replace
     * @param newHandlers the new handlers to insert.
     * @return this
     * @throws IllegalArgumentException is the oldHandler isn't part of this
     *                                  pipeline.
     */
    OutboundPipeline replace(OutboundHandler oldHandler, OutboundHandler... newHandlers);

    /**
     * Removes the given handler from the pipeline.
     *
     * This method should only be made on the thread 'owning' the pipeline.
     *
     * @param handler the handler to remove.
     * @return this
     * @throws IllegalArgumentException is the handler isn't part of this
     *                                  pipeline.
     */
    OutboundPipeline remove(OutboundHandler handler);

    /**
     * Request to flush all data to flush from the handlers to
     * the network.
     *
     * It will cause at least one processing of the OutboundPipeline.
     *
     * This method is thread-safe and can safely be called from any thread.
     *
     * Calling it while there is nothing in the pipeline will not do any damage,
     * apart from consuming cpu cycles.
     *
     * This can be used for example, with protocol or handshaking. So imagine
     * there is a handshake decoder (e.g. protocol or TLS), that as soon as it
     * has received some data like 'hello', it wants to send back a message to
     * the other side as part of the handshake. This can be done by looking up
     * the handshake encoder, queue the message on that encoder and then call
     * this method. This will cause the outbound pipeline to get processed.
     *
     * @return this
     */
    OutboundPipeline wakeup();
}
