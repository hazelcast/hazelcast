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

package com.hazelcast.jmx;

import com.hazelcast.client.ClientEngine;
import com.hazelcast.core.HazelcastInstance;
import java.util.Hashtable;

import static com.hazelcast.jmx.ManagementService.quote;

/**
 * Management bean for {@link com.hazelcast.client.ClientEngine}
 */
@ManagedDescription("HazelcastInstance.ClientEngine")
public class ClientEngineMBean extends HazelcastMBean<ClientEngine> {

    public ClientEngineMBean(HazelcastInstance hazelcastInstance, ClientEngine clientEngine, ManagementService service) {
        super(clientEngine, service);

        Hashtable<String, String> properties = new Hashtable<String, String>(3);
        properties.put("type", quote("HazelcastInstance.ClientEngine"));
        properties.put("name", quote(hazelcastInstance.getName()));
        properties.put("instance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("clientEndpointCount")
    @ManagedDescription("The number of client endpoints")
    public int getClientEndpointCount() {
        return managedObject.getClientEndpointCount();
    }
}
