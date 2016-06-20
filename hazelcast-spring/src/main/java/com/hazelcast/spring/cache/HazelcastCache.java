/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import java.util.concurrent.Callable;

/**
 * @author mdogan 4/3/12
 */
public class HazelcastCache implements Cache {

    private static final DataSerializable NULL = new NullDataSerializable();

    private final IMap<Object, Object> map;

    public HazelcastCache(final IMap<Object, Object> map) {
        this.map = map;
    }

    @Override
    public String getName() {
        return map.getName();
    }

    @Override
    public Object getNativeCache() {
        return map;
    }

    @Override
    public ValueWrapper get(final Object key) {
        if (key == null) {
            return null;
        }
        final Object value = lookup(key);
        return value != null ? new SimpleValueWrapper(fromStoreValue(value)) : null;
    }

    public <T> T get(Object key, Class<T> type) {
        Object value = fromStoreValue(lookup(key));
        if (type != null && value != null && !type.isInstance(value)) {
            throw new IllegalStateException("Cached value is not of required type [" + type.getName() + "]: " + value);
        }
        return (T) value;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(Object key, Callable<T> valueLoader) {
        Object value = lookup(key);
        if (value != null) {
            return (T) fromStoreValue(value);
        } else {
            this.map.lock(key);
            try {
                value = lookup(key);
                if (value != null) {
                    return (T) fromStoreValue(value);
                } else {
                    return loadValue(key, valueLoader);
                }
            } finally {
                this.map.unlock(key);
            }
        }
    }

    private <T> T loadValue(Object key, Callable<T> valueLoader) {
        T value;
        try {
            value = valueLoader.call();
        } catch (Exception ex) {
            throw new ValueRetrievalException(key, valueLoader, ex);
        }
        put(key, value);
        return value;
    }

    @Override
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

    @Override
    public void evict(final Object key) {
        if (key != null) {
            map.delete(key);
        }
    }

    @Override
    public void clear() {
        map.clear();
    }

    public ValueWrapper putIfAbsent(Object key, Object value) {
        Object result = map.putIfAbsent(key, toStoreValue(value));
        return result != null ? new SimpleValueWrapper(fromStoreValue(result)) : null;
    }

    private Object lookup(Object key) {
        return this.map.get(key);
    }

    static final class NullDataSerializable implements DataSerializable {
        @Override
        public void writeData(final ObjectDataOutput out) throws IOException {
        }

        @Override
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
