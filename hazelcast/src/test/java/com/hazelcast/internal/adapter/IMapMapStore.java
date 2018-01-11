/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class IMapMapStore implements DataStructureLoader, MapStore<Integer, String> {

    private volatile Collection<Integer> keys;

    @Override
    public void setKeys(Collection<Integer> keys) {
        this.keys = keys;
    }

    @Override
    public String load(Integer key) {
        if (keys == null) {
            return null;
        }
        return "newValue-" + key;
    }

    @Override
    public Map<Integer, String> loadAll(Collection<Integer> keys) {
        Map<Integer, String> map = new HashMap<Integer, String>(keys.size());
        for (Integer key : keys) {
            map.put(key, "newValue-" + key);
        }
        return map;
    }

    @Override
    public Iterable<Integer> loadAllKeys() {
        return keys;
    }

    @Override
    public void store(Integer key, String value) {
    }

    @Override
    public void storeAll(Map<Integer, String> map) {
    }

    @Override
    public void delete(Integer key) {
    }

    @Override
    public void deleteAll(Collection<Integer> keys) {
    }
}
