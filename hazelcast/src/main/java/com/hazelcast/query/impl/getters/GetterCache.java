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
import java.util.function.Supplier;

public interface GetterCache {
    int EVICTABLE_CACHE_MAX_CLASSES_IN_CACHE = 1000;
    int EVICTABLE_CACHE_MAX_GETTERS_PER_CLASS_IN_CACHE = 100;
    float EVICTABLE_CACHE_EVICTION_PERCENTAGE = 0.2f;

    Supplier<GetterCache> EVICTABLE_GETTER_CACHE_SUPPLIER = () -> new EvictableGetterCache(
            EVICTABLE_CACHE_MAX_CLASSES_IN_CACHE,
            EVICTABLE_CACHE_MAX_GETTERS_PER_CLASS_IN_CACHE,
            EVICTABLE_CACHE_EVICTION_PERCENTAGE,
            false
    );
    Supplier<GetterCache> SIMPLE_GETTER_CACHE_SUPPLIER = SimpleGetterCache::new;

    @Nullable
    Getter getGetter(Class<?> clazz, String attributeName);

    Getter putGetter(Class<?> clazz, String attributeName, Getter getter);
}
