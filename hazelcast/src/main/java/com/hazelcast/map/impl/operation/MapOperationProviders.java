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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;

/**
 * A central place to ask for {@link MapOperationProvider} instances.
 * {@link MapOperationProvider} instances created by this class will be wrapped by {@link WANAwareOperationProvider}
 * if WAN replication is enabled for the related map.
 */
public class MapOperationProviders {

    protected final MapServiceContext mapServiceContext;
    protected final MapOperationProvider wanAwareProvider;
    protected final MapOperationProvider defaultProvider = new DefaultMapOperationProvider();

    public MapOperationProviders(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.wanAwareProvider = new WANAwareOperationProvider(mapServiceContext, defaultProvider);
    }

    /**
     * Creates {@link MapOperationProvider} instance and wraps it into a {@link WANAwareOperationProvider}
     *
     * @param name Name of the requested {@link com.hazelcast.core.IMap}
     * @return {@link DefaultMapOperationProvider} or {@link WANAwareOperationProvider} depending on the WAN replication
     * config of the requested map instance.
     */
    public MapOperationProvider getOperationProvider(String name) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        return mapContainer.isWanReplicationEnabled() ? wanAwareProvider : defaultProvider;
    }
}
