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

package com.hazelcast.cache.impl;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;

import java.util.Map;
import java.util.Set;

/**
 * Static util methods for {@linkplain com.hazelcast.cache.ICache} implementations.
 *
 * @see com.hazelcast.cache.impl.CacheProxy
 */
public final class CacheProxyUtil {

    public static final int AWAIT_COMPLETION_TIMEOUT_SECONDS = 60;

    static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";

    private CacheProxyUtil() {
    }

    /**
     * Cache clear response validator, loop on results to validate that no exception exists on the result map.
     * Throws the first exception in the map.
     *
     * @param results map of {@link CacheClearResponse}.
     */
    public static void validateResults(Map<Integer, Object> results) {
        for (Object result : results.values()) {
            if (result != null && result instanceof CacheClearResponse) {
                final Object response = ((CacheClearResponse) result).getResponse();
                if (response instanceof Throwable) {
                    ExceptionUtil.sneakyThrow((Throwable) response);
                }
            }
        }
    }

    protected static int getPartitionId(NodeEngine nodeEngine, Data key) {
        return nodeEngine.getPartitionService().getPartitionId(key);
    }

    /**
     * Validates that a key is not null.
     * @param key the key to be validated.
     * @param <K> the type of key.
     * @throws java.lang.NullPointerException if provided key is null.
     */
    public static <K> void validateNotNull(K key) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
    }

    /**
     * Validates that key, value pair are both not null.
     *
     * @param key the key to be validated.
     * @param <K> the type of key.
     * @param value the value to be validated.
     * @param <V> the type of value.
     * @throws java.lang.NullPointerException if key or value is null.
     */
    public static <K, V> void validateNotNull(K key, V value) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
    }

    /**
     * Validates that key and multi values are not null.
     *
     * @param key the key to be validated.
     * @param value1 first value to be validated.
     * @param value2 second value to be validated.
     * @param <K> the type of key.
     * @param <V> the type of value.
     * @throws java.lang.NullPointerException if key or any value is null.
     */
    public static <K, V> void validateNotNull(K key, V value1, V value2) {
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value1 == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        if (value2 == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
    }

    /**
     * Validates that none of the keys are null in set.
     *
     * @param keys set of keys to be validated.
     * @param <K> the type of key.
     * @throws java.lang.NullPointerException if provided key set contains a null key.
     */
    public static <K> void validateNotNull(Set<? extends K> keys) {
        if (keys == null || keys.contains(null)) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
    }

    /**
     * This validator ensures that no key or value is null in the provided map.
     *
     * @param map the map to be validated.
     * @param <K> the type of key.
     * @param <V> the type of value.
     * @throws java.lang.NullPointerException if provided map contains a null key or value in the map.
     */
    public static <K, V> void validateNotNull(Map<? extends K, ? extends V> map) {
        if (map == null) {
            throw new NullPointerException("map is null");
        }
        boolean containsNullKey = false;
        boolean containsNullValue = false;
        //in case this map keySet or values do not support null values
        //TODO is it possible to validate a map without try-catch blocks, more efficiently?
        try {
            containsNullKey = map.keySet().contains(null);
        } catch (NullPointerException e) {
            //ignore as null not allowed for this map
            EmptyStatement.ignore(e);
        }
        try {
            containsNullValue = map.values().contains(null);
        } catch (NullPointerException e) {
            //ignore as null not allowed for this map
            EmptyStatement.ignore(e);
        }
        if (containsNullKey) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (containsNullValue) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }

    }

    /**
     * Validates that the configured key matches the provided key.
     * @param cacheConfig Cache configuration.
     * @param key the key to be validated with its type.
     * @param <K> the type of key.
     * @throws ClassCastException if the provided key does not match with configured type.
     */
    public static <K> void validateConfiguredTypes(CacheConfig cacheConfig, K key)
            throws ClassCastException {
        final Class keyType = cacheConfig.getKeyType();
        validateConfiguredKeyType(keyType, key);
    }

    /**
     * Validates the configured key and value types matches the provided key, value types.
     *
     * @param cacheConfig Cache configuration.
     * @param key the key to be validated.
     * @param <K> the type of key.
     * @param value the value to be validated.
     * @param <V> the type of value.
     * @throws ClassCastException if the provided key or value do not match with configured types.
     */
    public static <K, V> void validateConfiguredTypes(CacheConfig cacheConfig, K key, V value)
            throws ClassCastException {
        final Class keyType = cacheConfig.getKeyType();
        final Class valueType = cacheConfig.getValueType();
        validateConfiguredKeyType(keyType, key);
        validateConfiguredValueType(valueType, value);
    }

    /**
     * Validates the configured key and value types matches the provided key, value types.
     *
     * @param cacheConfig Cache configuration.
     * @param key the key to be validated.
     * @param value1 value to be validated.
     * @param value2 value to be validated.
     * @param <K> the type of key.
     * @param <V> the type of value.
     * @throws ClassCastException if the provided key or value do not match with configured types.
     */
    public static <K, V> void validateConfiguredTypes(CacheConfig cacheConfig, K key, V value1, V value2)
            throws ClassCastException {
        final Class keyType = cacheConfig.getKeyType();
        final Class valueType = cacheConfig.getValueType();
        validateConfiguredKeyType(keyType, key);
        validateConfiguredValueType(valueType, value1);
        validateConfiguredValueType(valueType, value2);
    }

    /**
     * Validates the key with key type.
     * @param keyType key class.
     * @param key key to be validated.
     * @param <K> the type of key.
     * @throws ClassCastException if the provided key do not match with keyType.
     */
    public static <K> void validateConfiguredKeyType(Class<K> keyType, K key)
            throws ClassCastException {
        if (Object.class != keyType) {
            //means type checks required
            if (!keyType.isAssignableFrom(key.getClass())) {
                throw new ClassCastException("Key " + key + "is not assignable to " + keyType);
            }
        }
    }

    /**
     * Validates the value with value type.
     * @param valueType value class.
     * @param value value to be validated.
     * @param <V> the type of value.
     * @throws ClassCastException if the provided value do not match with valueType.
     */
    public static <V> void validateConfiguredValueType(Class<V> valueType, V value)
            throws ClassCastException {
        if (Object.class != valueType) {
            //means type checks required
            if (!valueType.isAssignableFrom(value.getClass())) {
                throw new ClassCastException("Value " + value + "is not assignable to " + valueType);
            }
        }
    }

}
