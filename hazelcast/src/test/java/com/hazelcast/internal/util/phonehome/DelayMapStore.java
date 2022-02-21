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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.map.MapStore;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.test.HazelcastTestSupport.sleepAtLeastMillis;

public class DelayMapStore implements MapStore<Object, Object> {
    Map<Object, Object> store = new ConcurrentHashMap<>();

    @Override
    public void store(Object key, Object value) {
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<Object, Object> map) {
        store.putAll(map);
    }

    @Override
    public void delete(Object key) {
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<Object> keys) {
        store.clear();
    }

    @Override
    public Object load(Object key) {
        sleepAtLeastMillis(200);
        return store.get(key);
    }

    @Override
    public Map<Object, Object> loadAll(Collection<Object> keys) {
        return store;
    }

    @Override
    public Iterable<Object> loadAllKeys() {
        return store.keySet();
    }
}
