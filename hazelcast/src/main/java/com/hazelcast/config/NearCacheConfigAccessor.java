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

import com.hazelcast.spi.annotation.PrivateApi;

/**
 * Accessor for the {@link EvictionConfig} of a {@link NearCacheConfig} to initialize the old default max size,
 * if no size was configured by the user.
 */
@PrivateApi
public final class NearCacheConfigAccessor {

    private NearCacheConfigAccessor() {
    }

    public static NearCacheConfig copyWithInitializedDefaultMaxSizeForOnHeapMaps(NearCacheConfig nearCacheConfig) {
        if (nearCacheConfig == null) {
            return null;
        }

        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();
        if (nearCacheConfig.getInMemoryFormat() == InMemoryFormat.NATIVE
                || evictionConfig.sizeConfigured) {
            return nearCacheConfig;
        }

        // create copy of eviction config
        EvictionConfig copyEvictionConfig = new EvictionConfig(evictionConfig)
                .setSize(MapConfig.DEFAULT_MAX_SIZE);

        // create copy of nearCache config and set eviction config
        return new NearCacheConfig(nearCacheConfig)
                .setEvictionConfig(copyEvictionConfig);
    }
}
