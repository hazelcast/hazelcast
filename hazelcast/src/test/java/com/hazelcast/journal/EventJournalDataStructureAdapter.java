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

package com.hazelcast.journal;

import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.internal.services.ObjectNamespace;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Abstracts the Hazelcast data structures with event journal support
 */
public interface EventJournalDataStructureAdapter<K, V, EJ_TYPE> extends EventJournalReader<EJ_TYPE> {

    V put(K key, V value);

    V put(K key, V value, long ttl, TimeUnit timeunit);

    void putAll(Map<K, V> map);

    void load(K key);

    void loadAll(Set<K> keys);

    ObjectNamespace getNamespace();

    Set<Map.Entry<K, V>> entrySet();

    V remove(K key);

    int size();

    V get(K key);
}
