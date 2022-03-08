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

package com.hazelcast.map;

import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;

/**
 * Utility accessors for IMap internals
 */
public final class IMapAccessors {

    private IMapAccessors() {
    }

    public static MapContainer getMapContainer(IMap map) {
        MapServiceContext mapServiceContext = getMapServiceContext(map);
        return mapServiceContext.getMapContainers().get(map.getName());
    }

    public static MapServiceContext getMapServiceContext(IMap map) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        RemoteService service = mapProxy.getService();
        MapService mapService = (MapService) service;
        return mapService.getMapServiceContext();
    }
}
