/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.util;

import java.util.List;

public final class ListTypeUtils {

    // Private constructor to prevent instantiation
    private ListTypeUtils() {
        throw new UnsupportedOperationException("Utility class, cannot be instantiated");
    }

    /**
     * Checks if a list's first element is the specified type.
     *
     * @param list The list to check.
     * @param type The class type to check against.
     * @param <T>  The type of elements in the list.
     * @return true if the list is not empty and first element is the specified type; false otherwise.
     */
    public static <T> boolean firstElementIsInstanceOf(List<?> list, Class<T> type) {
        if (list == null || list.isEmpty()) {
            // Cannot determine type for null or empty list
            return false;
        }

        // Check the type of the first element
        return type.isInstance(list.get(0));
    }
}
