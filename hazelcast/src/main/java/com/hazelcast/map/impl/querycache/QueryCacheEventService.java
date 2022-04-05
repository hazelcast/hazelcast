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

package com.hazelcast.map.impl.querycache;

import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.impl.eventservice.EventFilter;

import java.util.UUID;

/**
 * Event service abstraction to allow different type of implementations on
 * query cache subscriber and query cache publisher sides. Runs locally on
 * query-cache subscriber side which may be node or client. Responsible for
 * registering/removing listeners and publishing events locally etc.
 *
 * @param <E> type of event to be published.
 */
public interface QueryCacheEventService<E> {

    /**
     * Publishes query-cache events locally.
     *
     * @param mapName    underlying map name of query cache
     * @param cacheId    ID of the query cache
     * @param event      event to publish
     * @param orderKey   use same order key for events which are required to be ordered
     * @param extractors extractors for the query cache
     */
    void publish(String mapName, String cacheId, E event, int orderKey, Extractors extractors);

    /**
     * Adds the listener to listen underlying IMap on all nodes.
     *
     * @param mapName         underlying map name of query cache
     * @param cacheId         ID of the query cache
     * @param listenerAdapter listener adapter for the query-cache
     * @return ID of registered event listener
     */
    UUID addPublisherListener(String mapName, String cacheId, ListenerAdapter listenerAdapter);

    /**
     * Removes listener from underlying IMap
     *
     * @param mapName    underlying map name which query cache listens
     * @param cacheId    ID of the query cache
     * @param listenerId ID of registered listener
     * @return {@code true} if listener is de-registered, {@code false} otherwise
     */
    boolean removePublisherListener(String mapName, String cacheId, UUID listenerId);

    /**
     * Adds a user-defined listener to a query-cache. This listener is registered as a local listener
     * on subscriber side.
     *
     * @param mapName  underlying IMap name of query-cache
     * @param cacheId  ID of the query-cache
     * @param listener listener for receiving events
     * @return ID of registered event listener
     */
    UUID addListener(String mapName, String cacheId, MapListener listener);

    /**
     * Adds a user-defined listener to a query-cache. This listener is registered as a local listener
     * on subscriber side.
     *
     * @param mapName  underlying IMap name of query-cache
     * @param cacheId  ID of the query-cache
     * @param listener listener for receiving events
     * @param filter   used to filter events
     * @return ID of registered event listener
     */
    UUID addListener(String mapName, String cacheId, MapListener listener, EventFilter filter);

    /**
     * Removes listener from this event service.
     *
     * @param mapName    underlying IMap name of query-cache
     * @param cacheId    ID of the query cache
     * @param listenerId registration ID of listener
     * @return {@code true} if listener is removed successfully, {@code false} otherwise
     */
    boolean removeListener(String mapName, String cacheId, UUID listenerId);

    /**
     * Removes all user defined listeners associated to supplied {@code cacheId}
     *
     * @param mapName underlying IMap name of query-cache
     * @param cacheId ID of the query cache
     */
    void removeAllListeners(String mapName, String cacheId);

    /**
     * Returns {@code true} if this query-cache has at least one registered
     * user defined listener otherwise returns {@code false}.
     *
     * @param mapName underlying IMap name of query-cache
     * @param cacheId ID of the query-cache
     * @return {@code true} if this query-cache has at least one registered listener
     * otherwise returns {@code false}
     */
    boolean hasListener(String mapName, String cacheId);

    /**
     * Only sends events which wrap data to be put in a query cache.
     *
     * @param name      listener name
     * @param eventData the event data
     * @param orderKey  the order key for the event
     */
    void sendEventToSubscriber(String name, Object eventData, int orderKey);
}
