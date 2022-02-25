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

package com.hazelcast.config;

import com.hazelcast.internal.util.StringUtil;

import java.util.Arrays;

/**
 *
 * Control caching of de-serialized values. Caching makes query evaluation faster but it costs memory.
 *
 * To maintain reusability, cached values are used by read-only operations only and they are never
 * passed to user-code where they could be accidentally mutated. Users will always get a fresh object,
 * which they are free to mutate.
 *
 * It does not have any effect when {@link com.hazelcast.nio.serialization.Portable} serialization or
 * {@link InMemoryFormat#OBJECT} format is used.
 *
 * @since 3.6
 */
public enum CacheDeserializedValues {

    /**
     * Never cache de-serialized value
     *
     */
    NEVER,

    /**
     * Cache values only when using search indexes
     *
     */
    INDEX_ONLY,

    /**
     * Always cache de-serialized values
     *
     */
    ALWAYS;

    /**
     * Create instance from String
     *
     * @param string the string value to parse
     * @return instance of {@link CacheDeserializedValues}
     * @throws IllegalArgumentException when unknown value is passed
     */
    public static CacheDeserializedValues parseString(String string) {
        String upperCase = StringUtil.upperCaseInternal(string);
        if ("NEVER".equals(upperCase)) {
            return NEVER;
        } else if ("INDEX_ONLY".equals(upperCase) || "INDEX-ONLY".equals(upperCase)) {
            return INDEX_ONLY;
        } else if ("ALWAYS".equals(upperCase)) {
            return ALWAYS;
        } else {
            throw new IllegalArgumentException("Unknown CacheDeserializedValues option '" + string + "'. "
                    + "Possible options: " + Arrays.toString(CacheDeserializedValues.values()));
        }
    }
}
