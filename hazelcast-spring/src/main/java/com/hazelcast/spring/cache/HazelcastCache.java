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

package com.hazelcast.spring.cache;

import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import java.io.IOException;

/**
 * @author mdogan 4/3/12
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
    
    public <T> T get(Object key, Class<T> type) {
        Object value = fromStoreValue(this.map.get(key));
        if (type != null && value != null && !type.isInstance(value)) {
            throw new IllegalStateException("Cached value is not of required type [" + type.getName() + "]: " + value);
        }
        return (T) value;
    }

    public void put(final Object key, final Object value) {
        if (key != null) {
            map.set(key, toStoreValue(value));
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
            map.delete(key);
        }
    }

    public void clear() {
        map.clear();
    }

    final static class NullDataSerializable implements DataSerializable {
        public void writeData(final ObjectDataOutput out) throws IOException {
        }
        public void readData(final ObjectDataInput in) throws IOException {
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
