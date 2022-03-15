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

package com.hazelcast.internal.management.operation;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheEntryProcessorEntry;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.IOException;

public class GetCacheEntryViewEntryProcessor implements EntryProcessor<Object, Object, CacheEntryView>,
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
