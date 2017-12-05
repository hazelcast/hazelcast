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

package com.hazelcast.internal.management.request;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.instance.HazelcastInstanceCacheManager;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.serialization.InternalSerializationService;

import static com.hazelcast.util.JsonUtil.getString;

/**
 * Request for fetching cache entries.
 */
public class GetCacheEntryRequest implements ConsoleRequest {
    private String cacheName;
    private String type;
    private String key;

    public GetCacheEntryRequest() {
    }

    public GetCacheEntryRequest(String type, String cacheName, String key) {
        this.type = type;
        this.cacheName = cacheName;
        this.key = key;
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CACHE_ENTRY;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject root) throws Exception {
        InternalSerializationService serializationService = mcs.getHazelcastInstance().getSerializationService();
        HazelcastInstanceCacheManager cacheManager = mcs.getHazelcastInstance().getCacheManager();
        ICache<Object, Object> cache = cacheManager.getCache(cacheName);
        CacheProxy cacheProxy = cache.unwrap(CacheProxy.class);

        CacheEntryView cacheEntry = null;

        if ("string".equals(type)) {
            cacheEntry = cacheProxy.getEntryViewInternal(key);
        } else if ("long".equals(type)) {
            cacheEntry = cacheProxy.getEntryViewInternal(Long.valueOf(key));
        } else if ("integer".equals(type)) {
            cacheEntry = cacheProxy.getEntryViewInternal(Integer.valueOf(key));
        }

        JsonObject result = new JsonObject();
        if (cacheEntry != null) {
            Object value = serializationService.toObject(cacheEntry.getValue());
            result.add("cacheBrowse_value", value != null ? value.toString() : "null");
            result.add("cacheBrowse_class", value != null ? value.getClass().getName() : "null");
            result.add("date_cache_creation_time", Long.toString(cacheEntry.getCreationTime()));
            result.add("date_cache_expiration_time", Long.toString(cacheEntry.getExpirationTime()));
            result.add("cacheBrowse_hits", Long.toString(cacheEntry.getAccessHit()));
            result.add("date_cache_access_time", Long.toString(cacheEntry.getLastAccessTime()));
        }
        root.add("result", result);
    }

    @Override
    public void fromJson(JsonObject json) {
        cacheName = getString(json, "cacheName");
        type = getString(json, "type");
        key = getString(json, "key");
    }
}
