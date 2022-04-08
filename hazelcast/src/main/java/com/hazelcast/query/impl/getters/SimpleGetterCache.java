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

package com.hazelcast.query.impl.getters;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link GetterCache} that uses simple, non-concurrent HashMaps.
 */
@NotThreadSafe
class SimpleGetterCache implements GetterCache {
    private final Map<Class<?>, Map<String, Getter>> cache = new HashMap<>();

    @Nullable
    @Override
    public Getter getGetter(Class<?> clazz, String attributeName) {
        Map<String, Getter> getterMapForClass = cache.get(clazz);
        if (getterMapForClass == null) {
            return null;
        }
        return getterMapForClass.get(attributeName);
    }

    @Override
    public Getter putGetter(Class<?> clazz, String attributeName, Getter getter) {
        return cache.computeIfAbsent(clazz, aClass -> new HashMap<>()).putIfAbsent(attributeName, getter);
    }
}
