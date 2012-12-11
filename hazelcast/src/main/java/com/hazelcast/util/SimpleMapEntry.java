/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.core.MapEntry;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.map.proxy.MapProxy;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;

/**
 * @mdogan 7/31/12
 */
public class SimpleMapEntry implements MapEntry {

    final HazelcastInstanceImpl instance;
    final String name;
    final Object key;
    final Data value;

    public SimpleMapEntry(final HazelcastInstanceImpl instance, final String name, final Object key, final Data value) {
        this.instance = instance;
        this.name = name;
        this.key = key;
        this.value = value;
    }

    public Object getKey() {
        return key;
    }

    public Object getValue() {
        return IOUtil.toObject(value);
    }

    public Object setValue(Object newValue) {
        return ((MapProxy) instance.getOrCreateInstance(name)).put(key, newValue);
    }

    public long getCost() {
        return 0;
    }

    public long getCreationTime() {
        return 0;
    }

    public long getExpirationTime() {
        return 0;
    }

    public int getHits() {
        return 0;
    }

    public long getLastAccessTime() {
        return 0;
    }

    public long getLastStoredTime() {
        return 0;
    }

    public long getLastUpdateTime() {
        return 0;
    }

    public long getVersion() {
        return 0;
    }

    public boolean isValid() {
        return false;
    }

    @Override
    public String toString() {
        return "Map.Entry key=" + getKey() + ", value=" + getValue();
    }
}
