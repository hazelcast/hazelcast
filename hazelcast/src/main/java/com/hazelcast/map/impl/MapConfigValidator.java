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

package com.hazelcast.map.impl;

import com.hazelcast.config.InMemoryFormat;

import static com.hazelcast.config.InMemoryFormat.NATIVE;

/**
 * Validates map configuration.
 */
public final class MapConfigValidator {

    private MapConfigValidator() {
    }

    /**
     * Throws {@link IllegalArgumentException} if the supplied {@link InMemoryFormat} is {@link InMemoryFormat#NATIVE}
     *
     * @param inMemoryFormat supplied inMemoryFormat.
     */
    public static void checkInMemoryFormat(InMemoryFormat inMemoryFormat) {
        if (NATIVE == inMemoryFormat) {
            throw new IllegalArgumentException("NATIVE storage format is supported in Hazelcast Enterprise only. "
                    + "Make sure you have Hazelcast Enterprise JARs on your classpath!");
        }

    }
}
