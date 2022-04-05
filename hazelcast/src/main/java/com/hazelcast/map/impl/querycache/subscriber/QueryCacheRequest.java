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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Represents a user request for creating a {@link com.hazelcast.map.QueryCache QueryCache}.
 */
public class QueryCacheRequest {

    private IMap map;
    private String mapName;
    private String cacheName;
    private Predicate predicate;
    private MapListener listener;
    private Boolean includeValue;
    private QueryCacheContext context;
    private QueryCacheConfig queryCacheConfig;
    private boolean isUrgent;

    QueryCacheRequest() {
    }

    public static QueryCacheRequest newQueryCacheRequest() {
        return new QueryCacheRequest();
    }

    public QueryCacheRequest forMap(IMap map) {
        this.map = checkNotNull(map, "map cannot be null");
        this.mapName = map.getName();
        return this;
    }

    public QueryCacheRequest withCacheName(String cacheName) {
        this.cacheName = checkHasText(cacheName, "cacheName");
        return this;
    }

    public QueryCacheRequest withPredicate(Predicate predicate) {
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");

        this.predicate = predicate;
        return this;
    }

    public QueryCacheRequest withListener(MapListener listener) {
        this.listener = listener;
        return this;
    }

    public QueryCacheRequest withIncludeValue(Boolean includeValue) {
        this.includeValue = includeValue;
        return this;
    }

    public QueryCacheRequest withContext(QueryCacheContext context) {
        this.context = checkNotNull(context, "context can not be null");
        return this;
    }

    public QueryCacheRequest withQueryCacheConfig(QueryCacheConfig queryCacheConfig) {
        this.queryCacheConfig = checkNotNull(queryCacheConfig, "queryCacheConfig can not be null");
        return this;
    }

    public QueryCacheRequest urgent(boolean urgent) {
        this.isUrgent = urgent;
        return this;
    }

    public IMap getMap() {
        return map;
    }

    public String getMapName() {
        return mapName;
    }

    public String getCacheName() {
        return cacheName;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public MapListener getListener() {
        return listener;
    }

    public Boolean isIncludeValue() {
        return includeValue;
    }

    public QueryCacheContext getContext() {
        return context;
    }

    public QueryCacheConfig getQueryCacheConfig() {
        return queryCacheConfig;
    }

    public boolean isUrgent() {
        return isUrgent;
    }
}
