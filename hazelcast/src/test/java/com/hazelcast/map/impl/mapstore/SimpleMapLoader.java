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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.MapLoader;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class SimpleMapLoader implements MapLoader<Integer, Integer> {

    final int size;
    final boolean slow;

    SimpleMapLoader(int size, boolean slow) {
        this.size = size;
        this.slow = slow;
    }

    @Override
    public Integer load(Integer key) {
        return null;
    }

    @Override
    public Map<Integer, Integer> loadAll(Collection<Integer> keys) {

        if (slow) {
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Map<Integer, Integer> result = new HashMap<Integer, Integer>();
        for (Integer key : keys) {
            result.put(key, key);
        }
        return result;
    }

    @Override
    public Iterable<Integer> loadAllKeys() {

        Set<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < size; i++) {
            keys.add(i);
        }
        return keys;
    }
}
