/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Various collection utility methods, mainly targeted to use internally.
 */
public final class CollectionUtil {

    private CollectionUtil() {
    }

    /**
     * Returns {@code true} if the given collection is null or empty, otherwise returns {@code false}.
     *
     * @param collection the given collection.
     * @return {@code true} if collection is empty.
     */
    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }

    /**
     * Returns {@code true} if the given collection is not null and not empty, otherwise returns {@code false}.
     *
     * @param collection the given collection.
     * @return {@code true} if collection is not empty.
     */
    public static boolean isNotEmpty(Collection collection) {
        return !isEmpty(collection);
    }

    /**
     * Add value to list of values in the map. Creates a new list if it doesn't exist for the key
     *
     * @param map
     * @param key
     * @param value
     * @return the updated list of values.
     */
    public static <K, V> List<V> addToValueList(Map<K, List<V>> map, K key, V value) {

        List<V> values = map.get(key);
        if (values == null) {
            values = new ArrayList<V>();
            map.put(key, values);
        }
        values.add(value);

        return values;
    }
}
