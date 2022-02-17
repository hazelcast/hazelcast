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

package com.hazelcast.client.map.helpers;

import com.hazelcast.map.MapStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AMapStore implements MapStore<Object, Object> {

    public final Map store = new ConcurrentHashMap();

    public AMapStore() {
    }

    @Override
    public void store(Object key, Object value) {
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<Object, Object> map) {
        for (Map.Entry<Object, Object> kvp : map.entrySet()) {
            store(kvp.getKey(), kvp.getValue());
        }
    }

    @Override
    public void delete(Object key) {
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<Object> keys) {
        for (Object key : keys) {
            delete(key);
        }
    }

    @Override
    public Object load(Object key) {
        return store.get(key);
    }

    @Override
    public Map<Object, Object> loadAll(Collection<Object> keys) {
        Map result = new HashMap();
        for (Object key : keys) {
            final Object v = load(key);
            if (v != null) {
                result.put(key, v);
            }
        }
        return result;
    }

    @Override
    public Set<Object> loadAllKeys() {
        return store.keySet();
    }
}
