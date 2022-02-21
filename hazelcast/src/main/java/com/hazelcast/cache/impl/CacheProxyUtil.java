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

package com.hazelcast.cache.impl;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Static util methods for {@linkplain com.hazelcast.cache.ICache} implementations.
 *
 * @see com.hazelcast.cache.impl.CacheProxy
 */
public final class CacheProxyUtil {

    public static final int AWAIT_COMPLETION_TIMEOUT_SECONDS = 60;

    public static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    private static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    private static final String NULL_SET_IS_NOT_ALLOWED = "Null set is not allowed!";

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
                Object response = ((CacheClearResponse) result).getResponse();
                if (response instanceof Throwable) {
                    ExceptionUtil.sneakyThrow((Throwable) response);
                }
            }
        }
    }

    public static int getPartitionId(NodeEngine nodeEngine, Data key) {
        return nodeEngine.getPartitionService().getPartitionId(key);
    }

    /**
     * Validates that a key is not null.
     *
     * @param key the key to be validated.
     * @param <K> the type of key.
     * @throws java.lang.NullPointerException if provided key is null.
     */
    public static <K> void validateNotNull(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
    }

    /**
     * Validates that key, value pair are both not null.
     *
     * @param key   the key to be validated.
     * @param <K>   the type of key.
     * @param value the value to be validated.
     * @param <V>   the type of value.
     * @throws java.lang.NullPointerException if key or value is null.
     */
    public static <K, V> void validateNotNull(K key, V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
    }

    /**
     * Validates that key and multi values are not null.
     *
     * @param key    the key to be validated.
     * @param value1 first value to be validated.
     * @param value2 second value to be validated.
     * @param <K>    the type of key.
     * @param <V>    the type of value.
     * @throws java.lang.NullPointerException if key or any value is null.
     */
    public static <K, V> void validateNotNull(K key, V value1, V value2) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value1, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(value2, NULL_VALUE_IS_NOT_ALLOWED);
    }

    /**
     * Validates supplied set is not null.
     *
     * @param keys set of keys to be validated.
     * @param <K>  the type of key.
     * @throws java.lang.NullPointerException if provided key set is null.
     */
    public static <K> void validateNotNull(Set<? extends K> keys) {
        if (keys == null) {
            throw new NullPointerException(NULL_SET_IS_NOT_ALLOWED);
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
        checkNotNull(map, "map is null");

        boolean containsNullKey = false;
        boolean containsNullValue = false;
        // we catch possible NPE since the Map implementation could not support null values
        // TODO: is it possible to validate a map more efficiently without try-catch blocks?
        try {
            containsNullKey = map.containsKey(null);
        } catch (NullPointerException e) {
            // ignore if null key is not allowed for this map
            ignore(e);
        }
        try {
            containsNullValue = map.containsValue(null);
        } catch (NullPointerException e) {
            // ignore if null value is not allowed for this map
            ignore(e);
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
     *
     * @param cacheConfig Cache configuration.
     * @param key         the key to be validated with its type.
     * @param <K>         the type of key.
     * @throws ClassCastException if the provided key does not match with configured type.
     */
    public static <K> void validateConfiguredTypes(CacheConfig cacheConfig, K key) throws ClassCastException {
        Class keyType = cacheConfig.getKeyType();
        validateConfiguredKeyType(keyType, key);
    }

    /**
     * Validates the configured key and value types matches the provided key, value types.
     *
     * @param cacheConfig Cache configuration.
     * @param key         the key to be validated.
     * @param <K>         the type of key.
     * @param value       the value to be validated.
     * @param <V>         the type of value.
     * @throws ClassCastException if the provided key or value do not match with configured types.
     */
    public static <K, V> void validateConfiguredTypes(CacheConfig cacheConfig, K key, V value) throws ClassCastException {
        Class keyType = cacheConfig.getKeyType();
        Class valueType = cacheConfig.getValueType();
        validateConfiguredKeyType(keyType, key);
        validateConfiguredValueType(valueType, value);
    }

    /**
     * Validates the configured key and value types matches the provided key, value types.
     *
     * @param cacheConfig Cache configuration.
     * @param key         the key to be validated.
     * @param value1      value to be validated.
     * @param value2      value to be validated.
     * @param <K>         the type of key.
     * @param <V>         the type of value.
     * @throws ClassCastException if the provided key or value do not match with configured types.
     */
    public static <K, V> void validateConfiguredTypes(CacheConfig cacheConfig, K key, V value1, V value2)
            throws ClassCastException {
        Class keyType = cacheConfig.getKeyType();
        Class valueType = cacheConfig.getValueType();
        validateConfiguredKeyType(keyType, key);
        validateConfiguredValueType(valueType, value1);
        validateConfiguredValueType(valueType, value2);
    }

    /**
     * Validates the key with key type.
     *
     * @param keyType key class.
     * @param key     key to be validated.
     * @param <K>     the type of key.
     * @throws ClassCastException if the provided key do not match with keyType.
     */
    public static <K> void validateConfiguredKeyType(Class<K> keyType, K key) throws ClassCastException {
        if (Object.class != keyType) {
            // means that type checks is required
            if (!keyType.isAssignableFrom(key.getClass())) {
                throw new ClassCastException("Key '" + key + "' is not assignable to " + keyType);
            }
        }
    }

    /**
     * Validates the value with value type.
     *
     * @param valueType value class.
     * @param value     value to be validated.
     * @param <V>       the type of value.
     * @throws ClassCastException if the provided value do not match with valueType.
     */
    public static <V> void validateConfiguredValueType(Class<V> valueType, V value) throws ClassCastException {
        if (Object.class != valueType) {
            // means that type checks is required
            if (!valueType.isAssignableFrom(value.getClass())) {
                throw new ClassCastException("Value '" + value + "' is not assignable to " + valueType);
            }
        }
    }
}
