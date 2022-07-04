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

import com.hazelcast.map.IMap;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.internal.util.ExceptionUtil;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Spring related {@link Cache} implementation for Hazelcast.
 */
public class HazelcastCache implements Cache {

    private static final DataSerializable NULL = new NullDataSerializable();

    private final IMap<Object, Object> map;

    /**
     * Read timeout for cache value retrieval operations.
     * <p>
     * If {@code 0} or negative, get() operations block, otherwise uses getAsync() with defined timeout.
     */
    private long readTimeout;

    public HazelcastCache(IMap<Object, Object> map) {
        this.map = map;
    }

    @Override
    public String getName() {
        return map.getName();
    }

    @Override
    public IMap<Object, Object> getNativeCache() {
        return map;
    }

    @Override
    public ValueWrapper get(Object key) {
        if (key == null) {
            return null;
        }
        Object value = lookup(key);
        return value != null ? new SimpleValueWrapper(fromStoreValue(value)) : null;
    }

    @SuppressWarnings("unchecked")
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
            throw ValueRetrievalExceptionResolver.resolveException(key, valueLoader, ex);
        }
        put(key, value);
        return value;
    }

    @Override
    public void put(Object key, Object value) {
        if (key != null) {
            map.set(key, toStoreValue(value));
        }
    }

    protected Object toStoreValue(Object value) {
        if (value == null) {
            return NULL;
        }
        return value;
    }

    protected Object fromStoreValue(Object value) {
        if (NULL.equals(value)) {
            return null;
        }
        return value;
    }

    @Override
    public void evict(Object key) {
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
        if (readTimeout > 0) {
            try {
                return this.map.getAsync(key).toCompletableFuture().get(readTimeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException te) {
                throw new OperationTimeoutException(te.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw ExceptionUtil.rethrow(e);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        return this.map.get(key);
    }

    static final class NullDataSerializable implements DataSerializable {

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
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

    private static class ValueRetrievalExceptionResolver {

        static RuntimeException resolveException(Object key, Callable<?> valueLoader,
                                                 Throwable ex) {
            return new ValueRetrievalException(key, valueLoader, ex);
        }
    }

    /**
     * Set cache value retrieval timeout
     *
     * @param readTimeout cache value retrieval timeout in milliseconds. 0 or negative values disable timeout
     */
    public void setReadTimeout(long readTimeout) {
        this.readTimeout = readTimeout;
    }

    /**
     * Return cache retrieval timeout in milliseconds
     */
    public long getReadTimeout() {
        return readTimeout;
    }
}
