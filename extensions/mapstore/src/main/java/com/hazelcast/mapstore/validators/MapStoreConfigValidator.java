/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.mapstore.validators;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;

/**
 * Validates MapConfig object
 */
public final class MapStoreConfigValidator {

    private MapStoreConfigValidator() {
    }

    /**
     * Validate MapConfig of the given map
     */
    public static void validateMapStoreConfig(HazelcastInstance instance, String mapName) {
        MapConfig mapConfig = instance.getConfig().findMapConfig(mapName);
        if (!mapConfig.getMapStoreConfig().isOffload()) {
            throw new HazelcastException("Config for GenericMapStore must have `offload` property set to true");
        }
    }
}
