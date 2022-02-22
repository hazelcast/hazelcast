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

package com.hazelcast.query.impl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Provides utilities useful for on-heap memory cost estimation.
 */
@SuppressWarnings("checkstyle:magicnumber")
public final class IndexHeapMemoryCostUtil {

    // The costs below are obtained using JOL (Java Object Layout tool), they
    // include the cost of the padding induced by 8-byte alignment of objects
    // on the heap. Compressed pointers are assumed to be on.

    private static final int BASE_ARRAY_COST = 16;
    private static final int BASE_STRING_COST = 24 + BASE_ARRAY_COST;
    private static final int BASE_BIG_INTEGER_COST = 40 + BASE_ARRAY_COST;
    private static final int BASE_BIG_DECIMAL_COST = 40;
    private static final int BASE_CONCURRENT_HASH_MAP_COST = 64 + BASE_ARRAY_COST;
    private static final int BASE_CONCURRENT_SKIP_LIST_MAP_COST = 48;

    private static final int DATE_COST = 24;
    private static final int SQL_TIMESTAMP_COST = 32;
    private static final int CONCURRENT_HASH_MAP_ENTRY_COST = 32;
    private static final int CONCURRENT_SKIP_LIST_MAP_ENTRY_COST = 24;
    private static final int QUERY_ENTRY_COST = 32;
    private static final int CACHED_QUERYABLE_ENTRY_COST = 40;

    private static final Map<Class, Integer> KNOWN_FINAL_CLASSES_COSTS;

    static {
        KNOWN_FINAL_CLASSES_COSTS = new HashMap<Class, Integer>();
        KNOWN_FINAL_CLASSES_COSTS.put(Boolean.class, 16);
        KNOWN_FINAL_CLASSES_COSTS.put(Character.class, 16);
        KNOWN_FINAL_CLASSES_COSTS.put(Byte.class, 16);
        KNOWN_FINAL_CLASSES_COSTS.put(Short.class, 16);
        KNOWN_FINAL_CLASSES_COSTS.put(Integer.class, 16);
        KNOWN_FINAL_CLASSES_COSTS.put(Long.class, 24);
        KNOWN_FINAL_CLASSES_COSTS.put(Float.class, 16);
        KNOWN_FINAL_CLASSES_COSTS.put(Double.class, 24);
        KNOWN_FINAL_CLASSES_COSTS.put(UUID.class, 32);
    }

    // The costs below are very rough estimates, more precise answers require
    // expensive computations which we can't afford here.

    private static final int ROUGH_BIG_INTEGER_COST = BASE_BIG_INTEGER_COST + 16;
    private static final int ROUGH_BIG_DECIMAL_COST = BASE_BIG_DECIMAL_COST + ROUGH_BIG_INTEGER_COST;
    private static final int ROUGH_UNKNOWN_CLASS_COST = 24;

    private IndexHeapMemoryCostUtil() {
    }

    /**
     * Estimates the on-heap memory cost of the given value.
     *
     * @param value the value to estimate the cost of.
     * @return the estimated value cost.
     */
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:returncount"})
    public static long estimateValueCost(Object value) {
        if (value == null) {
            return 0;
        }
        Class<?> clazz = value.getClass();

        Integer cost = KNOWN_FINAL_CLASSES_COSTS.get(clazz);
        if (cost != null) {
            return cost;
        }

        if (value instanceof String) {
            return BASE_STRING_COST + ((String) value).length() * 2L;
        }

        if (value instanceof Timestamp) {
            return SQL_TIMESTAMP_COST;
        }

        if (value instanceof Date) {
            return DATE_COST;
        }

        if (clazz.isEnum()) {
            // enum values are shared, so they don't cost anything
            return 0;
        }

        if (value instanceof BigDecimal) {
            return ROUGH_BIG_DECIMAL_COST;
        }

        if (value instanceof BigInteger) {
            return ROUGH_BIG_INTEGER_COST;
        }

        return ROUGH_UNKNOWN_CLASS_COST;
    }

    /**
     * Estimates the on-heap memory cost of a map backing an index.
     *
     * @param size                      the size of the map to estimate the cost
     *                                  of.
     * @param ordered                   {@code true} if the index managing the
     *                                  map being estimated is ordered, {@code
     *                                  false} otherwise.
     * @param usesCachedQueryableEntries {@code true} if queryable entries indexed
     *                                  by the associated index are cached, {@code
     *                                  false} otherwise.
     * @return the estimated map cost.
     */
    public static long estimateMapCost(long size, boolean ordered, boolean usesCachedQueryableEntries) {
        long mapCost;
        if (ordered) {
            mapCost = BASE_CONCURRENT_SKIP_LIST_MAP_COST + size * CONCURRENT_SKIP_LIST_MAP_ENTRY_COST;
        } else {
            mapCost = BASE_CONCURRENT_HASH_MAP_COST + size * CONCURRENT_HASH_MAP_ENTRY_COST;
        }

        long queryableEntriesCost;
        if (usesCachedQueryableEntries) {
            queryableEntriesCost = size * CACHED_QUERYABLE_ENTRY_COST;
        } else {
            queryableEntriesCost = size * QUERY_ENTRY_COST;
        }

        return mapCost + queryableEntriesCost;
    }

}
