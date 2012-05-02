/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.core.IMap;
import com.hazelcast.nio.DataSerializable;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @mdogan 4/3/12
 */
public class HazelcastCache implements Cache {

    private static final DataSerializable NULL = new NullDataSerializable();

    private final IMap<Object, Object> map;

    public HazelcastCache(final IMap<Object, Object> map) {
        this.map = map;
    }

    public String getName() {
        return map.getName();
    }

    public Object getNativeCache() {
        return map;
    }

    public ValueWrapper get(final Object key) {
        if (key == null) {
            return null;
        }
        final Object value = map.get(key);
        return value != null ? new SimpleValueWrapper(fromStoreValue(value)) : null;
    }

    public void put(final Object key, final Object value) {
        if (key != null) {
            map.set(key, toStoreValue(value), 0, TimeUnit.SECONDS);
        }
    }

    protected Object toStoreValue(final Object value) {
        if (value == null) {
            return NULL;
        }
        return value;
    }

    protected Object fromStoreValue(final Object value) {
        if (NULL.equals(value)) {
            return null;
        }
        return value;
    }

    public void evict(final Object key) {
        if (key != null) {
            map.evict(key);
        }
    }

    public void clear() {
        map.clear();
    }

    final static class NullDataSerializable implements DataSerializable {
        public void writeData(final DataOutput out) throws IOException {
        }
        public void readData(final DataInput in) throws IOException {
        }
        @Override
        public boolean equals(Object obj) {
            return obj != null && obj.getClass() == getClass();
        }
        @Override
        public int hashCode() {
            return 0;
        }
    }
}
