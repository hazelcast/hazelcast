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

package com.hazelcast.internal.adapter;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.monitor.LocalMapStats;

import java.util.Map;
import java.util.Set;

/**
 * Abstracts the Hazelcast data structures with Near Cache support for the Near Cache usage.
 */
public interface DataStructureAdapter<K, V> {

    void clear();

    void set(K key, V value);

    V put(K key, V value);

    V get(K key);

    ICompletableFuture<V> getAsync(K key);

    void putAll(Map<K, V> map);

    Map<K, V> getAll(Set<K> keys);

    void remove(K key);

    LocalMapStats getLocalMapStats();

    boolean containsKey(K key);
}
