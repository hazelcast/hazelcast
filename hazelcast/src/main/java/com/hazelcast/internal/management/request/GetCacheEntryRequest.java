/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.cache.impl.HazelcastInstanceCacheManager;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.IOException;

import static com.hazelcast.internal.util.JsonUtil.getString;

/**
 * Request for fetching cache entries.
 */
public class GetCacheEntryRequest implements ConsoleRequest {

    private static final GetCacheEntryViewEntryProcessor ENTRY_PROCESSOR = new GetCacheEntryViewEntryProcessor();
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

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject root) {
        InternalSerializationService serializationService = mcs.getHazelcastInstance().getSerializationService();
        HazelcastInstanceCacheManager cacheManager = mcs.getHazelcastInstance().getCacheManager();
        ICache<Object, Object> cache = cacheManager.getCache(cacheName);
        CacheEntryView cacheEntry = null;

        if ("string".equals(type)) {
            cacheEntry = cache.invoke(key, ENTRY_PROCESSOR);
        } else if ("long".equals(type)) {
            cacheEntry = cache.invoke(Long.valueOf(key), ENTRY_PROCESSOR);
        } else if ("integer".equals(type)) {
            cacheEntry = cache.invoke(Integer.valueOf(key), ENTRY_PROCESSOR);
        }
        JsonObject result = new JsonObject();
        if (cacheEntry != null) {
            Object value = serializationService.toObject(cacheEntry.getValue());
            result.add("cacheBrowse_value", value != null ? value.toString() : "null");
            result.add("cacheBrowse_class", value != null ? value.getClass().getName() : "null");
            result.add("date_cache_creation_time", Long.toString(cacheEntry.getCreationTime()));
            result.add("date_cache_expiration_time", Long.toString(cacheEntry.getExpirationTime()));
            result.add("cacheBrowse_hits", Long.toString(cacheEntry.getHits()));
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

    public static class GetCacheEntryViewEntryProcessor implements EntryProcessor<Object, Object, CacheEntryView>,
            IdentifiedDataSerializable, ReadOnly {
        @Override
        public CacheEntryView process(MutableEntry mutableEntry, Object... objects) throws EntryProcessorException {
            CacheEntryProcessorEntry entry = (CacheEntryProcessorEntry) mutableEntry;
            if (entry.getRecord() == null) {
                return null;
            }

            return new CacheBrowserEntryView(entry);
        }

        @Override
        public int getFactoryId() {
            return CacheDataSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return CacheDataSerializerHook.GET_CACHE_ENTRY_VIEW_PROCESSOR;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    public static class CacheBrowserEntryView implements CacheEntryView<Object, Object>, IdentifiedDataSerializable {
        private Object value;
        private long expirationTime;
        private long creationTime;
        private long lastAccessTime;
        private long accessHit;
        private ExpiryPolicy expiryPolicy;

        public CacheBrowserEntryView() {
        }

        CacheBrowserEntryView(CacheEntryProcessorEntry entry) {
            this.value = entry.getValue();

            CacheRecord<Object, ExpiryPolicy> record = entry.getRecord();
            this.expirationTime = record.getExpirationTime();
            this.creationTime = record.getCreationTime();
            this.lastAccessTime = record.getLastAccessTime();
            this.accessHit = record.getHits();
            this.expiryPolicy = record.getExpiryPolicy();
        }

        @Override
        public Object getKey() {
            return null;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public long getExpirationTime() {
            return expirationTime;
        }

        @Override
        public long getCreationTime() {
            return creationTime;
        }

        @Override
        public long getLastAccessTime() {
            return lastAccessTime;
        }

        @Override
        public long getHits() {
            return accessHit;
        }

        @Override
        public ExpiryPolicy getExpiryPolicy() {
            return expiryPolicy;
        }

        @Override
        public int getFactoryId() {
            return CacheDataSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return CacheDataSerializerHook.CACHE_BROWSER_ENTRY_VIEW;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(value);
            out.writeLong(expirationTime);
            out.writeLong(creationTime);
            out.writeLong(lastAccessTime);
            out.writeLong(accessHit);
            out.writeObject(expiryPolicy);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            value = in.readObject();
            expirationTime = in.readLong();
            creationTime = in.readLong();
            lastAccessTime = in.readLong();
            accessHit = in.readLong();
            expiryPolicy = in.readObject();
        }
    }
}
