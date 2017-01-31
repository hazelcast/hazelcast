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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.Predicate;

/**
 * Represents a user request for creating a {@link com.hazelcast.map.QueryCache QueryCache}.
 */
public interface QueryCacheRequest {

    /**
     * Setter for underlying {@link IMap} for this {@link com.hazelcast.map.QueryCache QueryCache}.
     *
     * @param map underlying {@link IMap}
     * @return this request object.
     */
    QueryCacheRequest forMap(IMap map);

    /**
     * Setter for name of the {@link com.hazelcast.map.QueryCache QueryCache}.
     *
     * @param cacheName name of {@link com.hazelcast.map.QueryCache QueryCache}
     * @return this request object.
     */
    QueryCacheRequest withCacheName(String cacheName);


    /**
     * Sets the user given name for the {@link com.hazelcast.map.QueryCache QueryCache}.
     *
     * @param userGivenCacheName name of {@link com.hazelcast.map.QueryCache QueryCache}
     * @return this request object.
     */
    QueryCacheRequest withUserGivenCacheName(String userGivenCacheName);

    /**
     * Setter for the predicate which will be used to filter underlying {@link IMap}
     * of this {@link com.hazelcast.map.QueryCache QueryCache}.
     *
     * @param predicate the predicate
     * @return this request object.
     */
    QueryCacheRequest withPredicate(Predicate predicate);

    /**
     * Setter for the listener which will be used to listen the {@link com.hazelcast.map.QueryCache QueryCache}.
     *
     * @param listener the listener
     * @return this request object.
     */
    QueryCacheRequest withListener(MapListener listener);

    /**
     * Setter for the {@link com.hazelcast.map.QueryCache QueryCache} for deciding
     * whether the implementation should cache values of entries.
     *
     * @param includeValue {@code true} for caching values in the {@link com.hazelcast.map.QueryCache QueryCache}
     *                     otherwise {@code false} to cache only keys.
     * @return this request object.
     */
    QueryCacheRequest withIncludeValue(Boolean includeValue);

    /**
     * The {@link QueryCacheContext} on this node/client.
     *
     * @param context the {@link QueryCacheContext}
     * @return this request object.
     */
    QueryCacheRequest withContext(QueryCacheContext context);

    /**
     * Returns underlying {@link IMap}.
     *
     * @return underlying {@link IMap}
     */
    IMap getMap();

    /**
     * Returns name of underlying {@link IMap}.
     *
     * @return name of underlying {@link IMap}
     */
    String getMapName();

    /**
     * Returns name of the {@link com.hazelcast.map.QueryCache QueryCache} to be created.
     *
     * @return name of the {@link com.hazelcast.map.QueryCache QueryCache} to be created.
     */
    String getCacheName();

    /**
     * Returns the name which is given by the user.
     *
     * @return user given name of the {@link com.hazelcast.map.QueryCache QueryCache} to be created.
     */
    String getUserGivenCacheName();

    /**
     * Returns predicate which is set.
     *
     * @return the predicate.
     */
    Predicate getPredicate();

    /**
     * Returns the listener for the {@link com.hazelcast.map.QueryCache QueryCache} to be created.
     *
     * @return the listener.
     */
    MapListener getListener();

    /**
     * Returns {@code true} if values will be cached otherwise {@code false}.
     *
     * @return {@code true} if values will be cached otherwise {@code false}
     */
    Boolean isIncludeValue();

    /**
     * Returns the {@link QueryCacheContext} on this node/client.
     *
     * @return the {@link QueryCacheContext} on this node/client.
     */
    QueryCacheContext getContext();
}
