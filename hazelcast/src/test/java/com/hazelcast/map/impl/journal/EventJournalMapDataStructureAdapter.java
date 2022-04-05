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

package com.hazelcast.map.impl.journal;

import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.journal.EventJournalDataStructureAdapter;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.ringbuffer.ReadResultSet;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class EventJournalMapDataStructureAdapter<K, V>
        implements EventJournalDataStructureAdapter<K, V, EventJournalMapEvent<K, V>> {

    private final IMap<K, V> map;

    public EventJournalMapDataStructureAdapter(IMap<K, V> map) {
        this.map = map;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        return map.put(key, value);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        return map.put(key, value, ttl, timeunit);
    }

    @Override
    public void putAll(Map<K, V> map) {
        this.map.putAll(map);
    }

    @Override
    public void load(K key) {
        map.get(key);
    }

    @Override
    public void loadAll(Set<K> keys) {
        map.loadAll(keys, true);
    }

    @Override
    public ObjectNamespace getNamespace() {
        return MapService.getObjectNamespace(map.getName());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public V remove(K key) {
        return map.remove(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletionStage<EventJournalInitialSubscriberState> subscribeToEventJournal(int partitionId) {
        return ((EventJournalReader<?>) map).subscribeToEventJournal(partitionId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CompletionStage<ReadResultSet<T>> readFromEventJournal(
            long startSequence, int minSize, int maxSize, int partitionId,
            java.util.function.Predicate<? super EventJournalMapEvent<K, V>> predicate,
            java.util.function.Function<? super EventJournalMapEvent<K, V>, ? extends T> projection) {
        return ((EventJournalReader<EventJournalMapEvent<K, V>>) map)
                .readFromEventJournal(startSequence, minSize, maxSize, partitionId, predicate, projection);
    }
}
