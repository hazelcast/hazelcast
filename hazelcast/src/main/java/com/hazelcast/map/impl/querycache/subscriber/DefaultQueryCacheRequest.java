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
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@code QueryCacheRequest}.
 *
 * @see QueryCacheRequest
 */
class DefaultQueryCacheRequest implements QueryCacheRequest {

    private IMap map;
    private Predicate predicate;
    private Boolean includeValue;
    private MapListener listener;
    private QueryCacheContext context;
    private String mapName;
    private String cacheId;
    private String cacheName;

    DefaultQueryCacheRequest() {
    }

    @Override
    public QueryCacheRequest forMap(IMap map) {
        this.map = checkNotNull(map, "map cannot be null");
        this.mapName = map.getName();
        return this;
    }

    @Override
    public QueryCacheRequest withCacheId(String cacheId) {
        this.cacheId = checkHasText(cacheId, "cacheId");
        return this;
    }

    @Override
    public QueryCacheRequest withCacheName(String cacheName) {
        this.cacheName = checkHasText(cacheName, "cacheName");
        return this;
    }

    @Override
    public QueryCacheRequest withPredicate(Predicate predicate) {
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");

        this.predicate = predicate;
        return this;
    }

    @Override
    public QueryCacheRequest withListener(MapListener listener) {
        this.listener = listener;
        return this;
    }

    @Override
    public QueryCacheRequest withIncludeValue(Boolean includeValue) {
        this.includeValue = includeValue;
        return this;
    }

    @Override
    public QueryCacheRequest withContext(QueryCacheContext context) {
        this.context = checkNotNull(context, "context can not be null");
        return this;
    }

    @Override
    public IMap getMap() {
        return map;
    }

    @Override
    public String getMapName() {
        return mapName;
    }

    @Override
    public String getCacheId() {
        return cacheId;
    }

    @Override
    public String getCacheName() {
        return cacheName;
    }

    @Override
    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    public MapListener getListener() {
        return listener;
    }

    @Override
    public Boolean isIncludeValue() {
        return includeValue;
    }

    @Override
    public QueryCacheContext getContext() {
        return context;
    }
}
