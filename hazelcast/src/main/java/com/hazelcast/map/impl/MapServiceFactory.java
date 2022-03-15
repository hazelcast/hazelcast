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

/**
 * Factory which is used to create a {@link MapService} object
 * and also aware of its {@link MapServiceContext}.
 *
 * @see MapServiceContext
 * @see MapService
 */
public interface MapServiceFactory {

    /**
     * Returns a {@link NodeEngine} implementation.
     *
     * @return {@link NodeEngine} implementation
     */
    NodeEngine getNodeEngine();

    /**
     * Returns a {@link MapServiceContext} implementation.
     *
     * @return {@link MapServiceContext} implementation
     */
    MapServiceContext getMapServiceContext();

    /**
     * Returns a {@link MapService} object.
     *
     * @return {@link MapService} object
     */
    MapService createMapService();
}
