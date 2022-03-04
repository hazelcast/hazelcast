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

package com.hazelcast.client.test;

import com.hazelcast.map.MapStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class is for Non-java clients. Please do not remove or modify.
 */
public class SampleMapStore implements MapStore<String, String> {
    private ConcurrentMap<String, String> internalStore = new ConcurrentHashMap<String, String>();

    @Override
    public void store(String key, String value) {
        internalStore.put(key, value);
    }

    @Override
    public void storeAll(Map<String, String> map) {
        internalStore.putAll(map);
    }

    @Override
    public void delete(String key) {
        internalStore.remove(key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        for (String key : keys) {
            delete(key);
        }
    }

    @Override
    public String load(String key) {
        return internalStore.get(key);
    }

    @Override
    public Map<String, String> loadAll(Collection<String> keys) {
        HashMap<String, String> resultMap = new HashMap<String, String>();
        for (String key : keys) {
            resultMap.put(key, internalStore.get(key));
        }
        return resultMap;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return internalStore.keySet();
    }
}
