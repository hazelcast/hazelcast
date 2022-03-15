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

import com.hazelcast.config.Config;
import com.hazelcast.internal.dynamicconfig.ConfigurationService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * Config supplier, which helps to match type of the config to corresponding search method.
 *
 * @param <T> specifies type of the config
 * @since 3.11
 * @see ConfigSearch
 */
public interface ConfigSupplier<T extends IdentifiedDataSerializable> {
    /**
     * Get dynamic configuration for the given name
     *
     * @param configurationService configuration service
     * @param name data structure name
     * @return configuration if found, or <code>null</code>
     */
    @Nullable
    T getDynamicConfig(@Nonnull ConfigurationService configurationService, @Nonnull String name);

    /**
     * Get static configuration for the given name
     *
     * @param staticConfig static config
     * @param name data structure name
     * @return configuration if found, or <code>null</code>
     */
    @Nullable
    T getStaticConfig(@Nonnull Config staticConfig, @Nonnull String name);

    /**
     * Get all static configs for the given config type.
     * @param staticConfig static config
     * @return name-to-config map
     */
    Map<String, T> getStaticConfigs(@Nonnull Config staticConfig);
}
