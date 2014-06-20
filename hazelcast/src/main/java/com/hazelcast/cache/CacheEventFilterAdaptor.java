/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.EventFilter;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.EventType;
import java.io.IOException;

public class CacheEventFilterAdaptor<K, V> implements EventFilter, DataSerializable {

    private transient ICache<K, V> source;

    private boolean oldValueRequired;
//    private final boolean isSynchronous;

    private CacheEntryEventFilter<? super K, ? super V> cacheEntryEventFilter;

    private String cacheName;

    public CacheEventFilterAdaptor(ICache<K, V> source, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

        this.source = source;
        this.cacheEntryEventFilter = cacheEntryListenerConfiguration.getCacheEntryEventFilterFactory() == null
                ? null
                : cacheEntryListenerConfiguration.getCacheEntryEventFilterFactory().create();
        this.oldValueRequired = cacheEntryListenerConfiguration.isOldValueRequired();
//        this.isSynchronous = cacheEntryListenerConfiguration.isSynchronous();
    }

    @Override
    public boolean eval(Object arg) {
        throw new UnsupportedOperationException();
    }

    public boolean filterEventData(EventType eventType, K key, V newValue, V oldValue) {
        final CacheEntryEventImpl<K, V> event = new CacheEntryEventImpl<K, V>(source, eventType, key, newValue, oldValue);
        return /*(this.isSynchronous==isSynchronous) && */(cacheEntryEventFilter == null || cacheEntryEventFilter.evaluate(event));
    }

    public boolean isOldValueRequired() {
        return oldValueRequired;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(oldValueRequired);
        out.writeObject(cacheEntryEventFilter);
        out.writeUTF(source.getName());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.oldValueRequired = in.readBoolean();
        this.cacheEntryEventFilter = in.readObject();
        this.cacheName = in.readUTF();
    }
}
