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

package com.hazelcast.map.impl.querycache.publisher;

import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.UUID;
import java.util.function.Function;

/**
 * Context for a publisher.
 *
 * Responsible for providing functionality/services for {@link NonStopPublisherAccumulator} and
 * {@link BatchPublisherAccumulator}.
 */
public interface PublisherContext {

    /**
     * Returns {@link AccumulatorInfoSupplier} for this context.
     *
     * @return {@link AccumulatorInfoSupplier} for this context.
     * @see AccumulatorInfoSupplier
     */
    AccumulatorInfoSupplier getAccumulatorInfoSupplier();

    /**
     * Returns {@link MapPublisherRegistry} for this context.
     *
     * @return {@link MapPublisherRegistry} for this context.
     * @see MapPublisherRegistry
     */
    MapPublisherRegistry getMapPublisherRegistry();

    /**
     * Returns {@link MapListenerRegistry} for this context.
     *
     * @return {@link MapListenerRegistry} for this context.
     * @see MapListenerRegistry
     */
    MapListenerRegistry getMapListenerRegistry();

    /**
     * Returns a helper function in order to register listener to {@code IMap}
     *
     * @return a helper function in order to register listener to {@code IMap}
     */
    Function<String, UUID> getListenerRegistrator();

    /**
     * Returns {@link QueryCacheContext} on this node.
     *
     * @return {@link QueryCacheContext} on this node.
     */
    QueryCacheContext getContext();

    /**
     * Returns underlying {@code NodeEngine} for this node.
     *
     * @return underlying {@code NodeEngine} for this node.
     */
    NodeEngine getNodeEngine();

    /**
     * Handles a disconnected subscriber. This method will be used
     * to clean orphaned {@link com.hazelcast.map.QueryCache QueryCache} resources.
     *
     * @param uuid client or node UUID of subscriber.
     */
    void handleDisconnectedSubscriber(UUID uuid);

    /**
     * Handles a reconnected subscriber. This method will be used
     * to stop actions taken in {@code #handleDisconnectedSubscriber}.
     *
     * @param uuid client or node UUID of subscriber.
     * @see #handleDisconnectedSubscriber
     */
    void handleConnectedSubscriber(UUID uuid);

    /**
     * Flushes this publisher context.
     * For example during graceful shutdown we need to flush all events in the
     * registered accumulators of this context.
     */
    void flush();
}
