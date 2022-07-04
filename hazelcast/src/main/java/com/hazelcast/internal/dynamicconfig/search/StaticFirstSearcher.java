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
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.internal.dynamicconfig.ConfigurationService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.hazelcast.internal.config.ConfigUtils.lookupByPattern;
import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getBaseName;

class StaticFirstSearcher<T extends IdentifiedDataSerializable> implements Searcher<T> {
    private final ConfigurationService configurationService;
    private final Config staticConfig;
    private final ConfigPatternMatcher configPatternMatcher;

    StaticFirstSearcher(ConfigurationService configurationService, Config staticConfig,
                        ConfigPatternMatcher configPatternMatcher) {
        this.configurationService = configurationService;
        this.staticConfig = staticConfig;
        this.configPatternMatcher = configPatternMatcher;
    }

    @Override
    public T getConfig(@Nonnull String name, String fallbackName, @Nonnull ConfigSupplier<T> configSupplier) {
        String baseName = getBaseName(name);
        Map<String, T> staticCacheConfigs = configSupplier.getStaticConfigs(staticConfig);
        T config = lookupByPattern(configPatternMatcher, staticCacheConfigs, baseName);
        if (config == null) {
            config = configSupplier.getDynamicConfig(configurationService, baseName);
        }
        if (config == null && fallbackName != null) {
            config = configSupplier.getStaticConfig(staticConfig, fallbackName);
        }
        return config;
    }
}
