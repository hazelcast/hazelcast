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

package com.hazelcast.internal.util.collection;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Map with a composed key.
 *
 * It's not thread safe and it does not allow nulls.
 *
 * It's biased towards use-cases where you have a smaller set of first keys.
 *
 * Alternative implementation could avoid map nesting and wrap keys inside
 * a <code>ComposedKey</code> object.
 *
 * @param <K1>
 * @param <K2>
 * @param <V>
 */
public class ComposedKeyMap<K1, K2, V> {
    private final Map<K1, Map<K2, V>> backingMap;

    public ComposedKeyMap() {
        backingMap = new HashMap<K1, Map<K2, V>>();
    }

    public V put(K1 key1, K2 key2, V value) {
        checkNotNull(key1, "Key1 cannot be null");
        checkNotNull(key2, "Key2 cannot be null");
        checkNotNull(value, "Value cannot be null");

        Map<K2, V> innerMap = backingMap.get(key1);
        if (innerMap == null) {
            innerMap = new HashMap<K2, V>();
            backingMap.put(key1, innerMap);
        }
        return innerMap.put(key2, value);
    }

    public V get(K1 key1, K2 key2) {
        checkNotNull(key1, "Key1 cannot be null");
        checkNotNull(key2, "Key2 cannot be null");

        Map<K2, V> innerMap = backingMap.get(key1);
        if (innerMap == null) {
            return null;
        }
        return innerMap.get(key2);
    }
}
