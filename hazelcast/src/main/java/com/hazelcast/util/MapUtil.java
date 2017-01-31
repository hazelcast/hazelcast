/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for Maps
 */
public final class MapUtil {

    private static final double HASHMAP_DEFAULT_LOAD_FACTOR = 0.75;

    private MapUtil() { }

    /**
     * Utility method that creates an {@link java.util.HashMap} with its initialCapacity calculated
     * to minimize rehash operations
     */
    public static <K, V> Map<K, V> createHashMap(int expectedMapSize) {
        int initialCapacity = (int) (expectedMapSize / HASHMAP_DEFAULT_LOAD_FACTOR) + 1;
        return new HashMap<K, V>(initialCapacity);
    }

    /**
     * Test the given map and return {@code true} if the map is null or empty.
     * @param map the map to test
     * @return    {@code true} if {@code map} is null or empty, otherwise {@code false}.
     */
    public static boolean isNullOrEmpty(Map map) {
        return map == null || map.isEmpty();
    }

}
