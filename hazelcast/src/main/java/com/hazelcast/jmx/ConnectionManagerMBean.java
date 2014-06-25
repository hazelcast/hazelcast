/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.ConnectionManager;
import java.util.Hashtable;

import static com.hazelcast.jmx.ManagementService.quote;

/**
 * Management bean for {@link com.hazelcast.nio.ConnectionManager}
 */
@ManagedDescription("HazelcastInstance.ConnectionManager")
public class ConnectionManagerMBean extends HazelcastMBean<ConnectionManager> {

    public ConnectionManagerMBean(HazelcastInstance hazelcastInstance, ConnectionManager connectionManager,
                                  ManagementService service) {
        super(connectionManager, service);

        Hashtable<String, String> properties = new Hashtable<String, String>(3);
        properties.put("type", quote("HazelcastInstance.ConnectionManager"));
        properties.put("instance", quote(hazelcastInstance.getName()));
        properties.put("name", hazelcastInstance.getName());
        setObjectName(properties);
    }

    public ConnectionManager getConnectionManager() {
        return managedObject;
    }

    @ManagedAnnotation("clientConnectionCount")
    @ManagedDescription("Current number of client connections")
    public int getCurrentClientConnections() {
        return getConnectionManager().getCurrentClientConnections();
    }

    @ManagedAnnotation("activeConnectionCount")
    @ManagedDescription("Current number of active connections")
    public int getActiveConnectionCount() {
        return getConnectionManager().getActiveConnectionCount();
    }

    @ManagedAnnotation("connectionCount")
    @ManagedDescription("Current number of connections")
    public int getConnectionCount() {
        return getConnectionManager().getConnectionCount();
    }
}
