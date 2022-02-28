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

package com.hazelcast.map.impl;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.util.ConstructorFunction;

/**
 * Helper which is used to create a {@link MapService} object.
 */
public final class MapServiceConstructor {

    private static final ConstructorFunction<NodeEngine, MapService> DEFAULT_MAP_SERVICE_CONSTRUCTOR
            = nodeEngine -> {
                MapServiceContext defaultMapServiceContext = new MapServiceContextImpl(nodeEngine);
                MapServiceFactory factory
                        = new DefaultMapServiceFactory(nodeEngine, defaultMapServiceContext);
                return factory.createMapService();
            };

    private MapServiceConstructor() {
    }

    /**
     * Returns a {@link ConstructorFunction Constructor} which will be used to create a  {@link MapService} object.
     *
     * @return {@link ConstructorFunction Constructor} which will be used to create a  {@link MapService} object.
     */
    public static ConstructorFunction<NodeEngine, MapService> getDefaultMapServiceConstructor() {
        return DEFAULT_MAP_SERVICE_CONSTRUCTOR;
    }
}
