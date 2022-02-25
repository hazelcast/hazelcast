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

package com.hazelcast.internal.dynamicconfig.search;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;

/**
 * Performs search data structures configuration based on selected search method
 * @param <T> type of config to find
 * @since 3.11
 * @see ConfigSearch
 */
public interface Searcher<T extends IdentifiedDataSerializable> {
    /**
     * Get configuration for data structure with the given name
     *
     * @param name data structure name
     * @param fallbackName name to fallback if config for original name is not found
     * @param configSupplier data config supplier
     * @return configuration for <code>name</code> or fallback config for <code>fallbackName</code>. If neither is
     * found, <code>null</code> is returned.
     */
    T getConfig(@Nonnull String name, String fallbackName, @Nonnull ConfigSupplier<T> configSupplier);
}
