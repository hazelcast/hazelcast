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

import com.hazelcast.config.MapConfig;

import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateMaxIdleMillis;
import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateTTLMillis;

/**
 * Contains support methods of a map container.
 *
 * @see MapContainer
 */
public abstract class MapContainerSupport {

    protected volatile MapConfig mapConfig;

    private final long maxIdleMillis;

    private final long ttlMillisFromConfig;

    private final String name;

    protected MapContainerSupport(String name, MapConfig mapConfig) {
        this.name = name;
        this.mapConfig = mapConfig;
        this.maxIdleMillis = calculateMaxIdleMillis(mapConfig);
        this.ttlMillisFromConfig = calculateTTLMillis(mapConfig);
    }

    public MapConfig getMapConfig() {
        return mapConfig;
    }

    public void setMapConfig(MapConfig mapConfig) {
        this.mapConfig = mapConfig;
    }

    public long getMaxIdleMillis() {
        return maxIdleMillis;
    }

    public long getTtlMillisFromConfig() {
        return ttlMillisFromConfig;
    }

    public String getName() {
        return name;
    }
}
