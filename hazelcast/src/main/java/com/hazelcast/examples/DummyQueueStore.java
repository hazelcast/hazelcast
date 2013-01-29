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

package com.hazelcast.examples;

import com.hazelcast.core.QueueStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @ali 12/14/12
 */
public class DummyQueueStore implements QueueStore<Object> {

    HashMap<Long, Object> map = new HashMap<Long, Object>();

    public DummyQueueStore() {
    }

    public void store(Long key, Object value) {
        map.put(key, value);
    }

    public void storeAll(Map<Long, Object> map) {
        this.map.putAll(map);
    }

    public void delete(Long key) {
        map.remove(key);
    }

    public void deleteAll(Collection<Long> keys) {
        for (Long key: keys){
            map.remove(key);
        }
    }

    public Object load(Long key) {
        return map.get(key);
    }

    public Map<Long, Object> loadAll(Collection<Long> keys) {
        Map<Long, Object> temp = new HashMap<Long, Object>(keys.size());
        for (Long key: keys){
            temp.put(key, map.get(key));
        }
        return temp;
    }

    public Set<Long> loadAllKeys() {
        return map.keySet();
    }
}
