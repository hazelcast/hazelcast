/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.examples;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DummyStore implements MapLoader, MapStore {

    public Set loadAllKeys() {
        System.out.println("Loader.loadAllKeys ");
        Set keys = new HashSet();
        keys.add("key");
        return keys;
    }

    public Object load(Object key) {
        System.out.println("Loader.load " + key);
        return "loadedvalue";
    }

    public Map loadAll(Collection keys) {
        System.out.println("Loader.loadAll keys " + keys);
        return null;
    }

    public void store(Object key, Object value) {
        System.out.println("Store.store key=" + key + ", value=" + value);
    }

    public void storeAll(Map map) {
        System.out.println("Store.storeAll " + map.size());
    }

    public void delete(Object key) {
        System.out.println("Store.delete " + key);
    }

    public void deleteAll(Collection keys) {
        System.out.println("Store.deleteAll " + keys);
    }
}
