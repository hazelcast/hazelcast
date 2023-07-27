/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

/**
 * Shared logic for PartitioningStrategies and classes using parts of their logic.
 */
public final class PartitioningStrategyUtil {
    private PartitioningStrategyUtil() { }

    /**
     * Constructs a PartitioningKey depending on the number of attribute values - either returns a singular object
     * or an array of attribute values. Kept in this utility class as a way to centralize this logic.
     *
     * @param keyAttributes - array of key attribute values (e.g. POJO field values)
     * @return single attribute value Object or an Object[] array of attribute values.
     */
    public static Object constructAttributeBasedKey(Object[] keyAttributes) {
        return keyAttributes.length == 1 ? keyAttributes[0] : keyAttributes;
    }
}
