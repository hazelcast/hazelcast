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
 * The InboundPipeline is pipeline responsible for inbound traffic.
 *
 * For example there could be a TLSDecoder followed by a PacketDecoder.
 *
 * A InboundPipeline contains a sequence of {@link InboundHandler}
 * instances and the pipeline can be dynamically be modified.
 *
 * <h1>Spurious wakeups</h1>
 * InboundHandlers/OutboundHandlers need to be able to deal with
 * spurious wakeups. E.g. it could be that a PacketDecoder is called without
 * any Packets being available.
 *
 * <h1>Automatic reprocessing on change</h1>
 * When a change in the pipeline is detected, the pipeline is automatically
 * reprocessed. For example when the ProtocolDecoder replaced itself by a
 * PacketDecoder, the whole pipeline (in this case the PacketDecoder) is
 * automatically reprocessed.
 */
public interface InboundPipeline {

    /**
     * Adds the handlers at the end of the pipeline.
     *
     * No verification is done if the handler is already added and a handler
     * should only be added once.
     *
     * This method should only be made on the thread 'owning' the pipeline.
     *
     * @param handlers the handlers to add
     * @return this
     */
    InboundPipeline addLast(InboundHandler... handlers);

    /**
     * Replaces the old InboundHandler by the new ones. So if there
     * is a sequence of handlers [H1,H2,H3] and H2 gets replaced by [H4,H5]
     * the new pipeline will be [H1,H4,H5,H3].
     *
     * No verification is done if any of the handlers is already added and a
     * handler should only be added once.
     *
     * This method should only be made on the thread 'owning' the pipeline.
     *
     * @param oldHandler  the handler to replace
     * @param newHandlers the new handlers to insert
     * @return this
     * @throws IllegalArgumentException is the oldHandler isn't part of this
     *                                  pipeline.
     */
    InboundPipeline replace(InboundHandler oldHandler, InboundHandler... newHandlers);

    /**
     * Removes the given handler from the pipeline.
     *
     * This method should only be made on the thread 'owning' the pipeline.
     *
     * @param handler the handler to remove
     * @return this
     * @throws IllegalArgumentException is the handler isn't part of this
     *                                  pipeline.
     */
    InboundPipeline remove(InboundHandler handler);


    /**
     * Wakes up the inbound pipeline and lets it start reading again from the
     * network.
     *
     * Even if there is no data to be read, it will cause at least one processing
     * of the InboundPipeline. This will force any buffered data to be pushed
     * through the InboundPipeline.
     *
     * This method is thread-safe and can safely be called from any thread.
     *
     * Calling it while it is already waken up will not do any damage, it will
     * just cause some temporary overhead.
     *
     * This method is useful for example to restart the pipeline after a task that
     * has been offloaded, completes.
     *
     * @return this
     */
    InboundPipeline wakeup();
}
