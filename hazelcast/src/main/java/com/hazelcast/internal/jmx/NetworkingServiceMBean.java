/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.internal.nio.NetworkingService;

import java.util.Map;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.internal.jmx.ManagementService.quote;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Management bean for {@link NetworkingService}
 */
@ManagedDescription("HazelcastInstance.NetworkingService")
public class NetworkingServiceMBean
        extends HazelcastMBean<NetworkingService> {

    private static final int PROPERTY_COUNT = 3;

    public NetworkingServiceMBean(HazelcastInstance hazelcastInstance, NetworkingService ns,
                                  ManagementService service) {
        super(ns, service);

        //no need to create HashTable here, as the setObjectName method creates a copy of the given properties
        final Map<String, String> properties = createHashMap(PROPERTY_COUNT);
        properties.put("type", quote("HazelcastInstance.NetworkingService"));
        properties.put("instance", quote(hazelcastInstance.getName()));
        properties.put("name", quote(hazelcastInstance.getName()));
        setObjectName(properties);
    }

    public NetworkingService getNetworkingService() {
        return managedObject;
    }

    @ManagedAnnotation("clientConnectionCount")
    @ManagedDescription("Current number of client connections")
    public int getCurrentClientConnections() {
        EndpointManager cem = getNetworkingService().getEndpointManager(CLIENT);
        if (cem == null) {
            return -1;
        }

        return cem.getActiveConnections().size();
    }

    @ManagedAnnotation("activeConnectionCount")
    @ManagedDescription("Current number of active connections")
    public int getActiveConnectionCount() {
        return getNetworkingService().getAggregateEndpointManager().getActiveConnections().size();
    }

    @ManagedAnnotation("connectionCount")
    @ManagedDescription("Current number of connections")
    public int getConnectionCount() {
        return getNetworkingService().getAggregateEndpointManager().getConnections().size();
    }
}
