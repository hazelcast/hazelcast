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

package com.hazelcast.cache.impl.journal;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.journal.EventJournalDataStructureAdapter;
import com.hazelcast.ringbuffer.ReadResultSet;

import javax.cache.Cache;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

public class EventJournalCacheDataStructureAdapter<K, V>
        implements EventJournalDataStructureAdapter<K, V, EventJournalCacheEvent<K, V>> {

    private final ICache<K, V> cache;

    public EventJournalCacheDataStructureAdapter(ICache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public V put(K key, V value) {
        return cache.getAndPut(key, value);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        return cache.getAndPut(key, value, new HazelcastExpiryPolicy(ttl, ttl, ttl, timeunit));
    }

    @Override
    public void putAll(Map<K, V> map) {
        cache.putAll(map);
    }

    @Override
    public void load(K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadAll(Set<K> keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectNamespace getNamespace() {
        return CacheService.getObjectNamespace(cache.getPrefixedName());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return getEntries(cache);
    }

    @Override
    public V remove(K key) {
        return cache.getAndRemove(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletionStage<EventJournalInitialSubscriberState> subscribeToEventJournal(int partitionId) {
        return ((EventJournalReader<?>) cache).subscribeToEventJournal(partitionId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CompletionStage<ReadResultSet<T>> readFromEventJournal(
            long startSequence,
            int minSize,
            int maxSize,
            int partitionId,
            Predicate<? super EventJournalCacheEvent<K, V>> predicate,
            Function<? super EventJournalCacheEvent<K, V>, ? extends T> projection
    ) {
        final EventJournalReader<EventJournalCacheEvent<K, V>> reader
                = (EventJournalReader<EventJournalCacheEvent<K, V>>) this.cache;
        return reader.readFromEventJournal(startSequence, minSize, maxSize, partitionId, predicate, projection);
    }

    private Set<Map.Entry<K, V>> getEntries(ICache<K, V> cache) {
        final Iterator<Cache.Entry<K, V>> it = cache.iterator();
        final HashSet<Entry<K, V>> entries = new HashSet<Map.Entry<K, V>>(cache.size());
        while (it.hasNext()) {
            final Cache.Entry<K, V> e = it.next();
            entries.add(new SimpleImmutableEntry<K, V>(e.getKey(), e.getValue()));
        }
        return entries;
    }
}
