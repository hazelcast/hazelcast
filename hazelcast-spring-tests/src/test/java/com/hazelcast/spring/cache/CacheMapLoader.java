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

package com.hazelcast.spring.cache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class CacheMapLoader implements MapStore, MapLoaderLifecycleSupport {

    private String type;

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        type = mapName;
    }

    @Override
    public void destroy() {
    }

    @Override
    public Object load(Object key) {
        return type + ":" + key;
    }

    @Override
    public Map loadAll(Collection keys) {
        return null;
    }

    @Override
    public Set loadAllKeys() {
        return null;
    }

    @Override
    public void store(Object key, Object value) {
    }

    @Override
    public void storeAll(Map map) {
    }

    @Override
    public void delete(Object key) {
    }

    @Override
    public void deleteAll(Collection keys) {
    }
}
