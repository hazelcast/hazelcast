/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.core;

import com.hazelcast.client.query.Predicate;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.Map;
import java.util.Collection;

public interface IMap<K, V> extends ConcurrentMap<K, V>, Instance {

    String getName();

    void lock(K key);

    boolean tryLock(K key);

    boolean tryLock(K key, long time, TimeUnit timeunit);

    void unlock(K key);

    void addEntryListener(EntryListener listener, boolean includeValue);

    void removeEntryListener(EntryListener listener);

    void addEntryListener(EntryListener listener, K key, boolean includeValue);

    void removeEntryListener(EntryListener listener, K key);

    MapEntry getMapEntry(K key);
    
    boolean evict(K key);

    Set<K> keySet(Predicate predicate);

    Set<Map.Entry<K, V>> entrySet(Predicate predicate);

    Collection<V> values (Predicate predicate);

}
