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

package com.hazelcast.jet.internal.impl.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class provides builder pattern to create LinkedHashMap
 *
 * @param <K> - type of map of the key
 * @param <V> - type of map of the value
 */
public final class LinkedMapBuilder<K, V> {
    private final Map<K, V> map;

    private LinkedMapBuilder() {
        map = new LinkedHashMap<K, V>();
    }

    public static <K, V> LinkedMapBuilder<K, V> builder() {
        return new LinkedMapBuilder<K, V>();
    }

    public static <K, V> Map<K, V> of(K key, V value) {
        return LinkedMapBuilder.<K, V>builder().put(key, value).build();
    }

    public static <K, V> Map<K, V> of(K key1, V value1, K key2, V value2) {
        return LinkedMapBuilder.<K, V>builder().put(key1, value1).put(key2, value2).build();
    }

    public static <K, V> Map<K, V> of(K key1, V value1, K key2, V value2, K key3, V value3) {
        return LinkedMapBuilder.<K, V>builder().put(key1, value1).put(key2, value2).put(key3, value3).build();
    }

    public static <K, V> Map<K, V> of(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4) {
        return LinkedMapBuilder.<K, V>builder().put(key1, value1).put(key2, value2).put(key3, value3).put(key4, value4).build();
    }

    //CHECKSTYLE:OFF
    public static <K, V> Map<K, V> of(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4, K key5, V value5) {
        return LinkedMapBuilder.<K, V>builder().
                put(key1, value1).
                put(key2, value2).
                put(key3, value3).
                put(key4, value4).
                put(key5, value5).
                build();
    }

    //CHECKSTYLE:OFF
    public static <K, V> Map<K, V> of(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4, K key5, V value5, K key6, V value6) {
        return LinkedMapBuilder.<K, V>builder().
                put(key1, value1).
                put(key2, value2).
                put(key3, value3).
                put(key4, value4).
                put(key5, value5).
                put(key6, value6).
                build();
    }

    public LinkedMapBuilder<K, V> put(K key, V value) {
        map.put(key, value);
        return this;
    }
    //CHECKSTYLE:ON

    public Map<K, V> build() {
        return map;
    }
    //CHECKSTYLE:ON
}
