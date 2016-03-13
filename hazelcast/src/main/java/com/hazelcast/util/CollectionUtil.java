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

package com.hazelcast.util;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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

    /**
     * Return n-th item or null if collection is smaller
     *
     * @param collection
     * @param position
     * @param <T>
     * @return
     */
    public static <T> T getItemAtPositionOrNull(Collection<T> collection, int position) {
        if (position >= collection.size()) {
            return null;
        }
        if (collection instanceof List) {
            return ((List<T>) collection).get(position);
        }
        Iterator<T> iterator = collection.iterator();
        T item = null;
        for (int i = 0; i < position + 1; i++) {
            item = iterator.next();
        }
        return item;
    }

    /**
     * @param collection           collection of objects to be converted to collection of data
     * @param serializationService will be used for converting object to data
     * @return collection of data
     * @throws java.lang.NullPointerException if collection is null, or contains a null element
     */
    public static <C> Collection<Data> objectToDataCollection(Collection<C> collection
            , SerializationService serializationService) {
        List<Data> dataKeys = new ArrayList<Data>(collection.size());
        for (C c : collection) {
            Preconditions.checkNotNull(c);
            dataKeys.add(serializationService.toData(c));
        }
        return dataKeys;
    }

}
