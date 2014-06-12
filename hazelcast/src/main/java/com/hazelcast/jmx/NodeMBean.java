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
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;

import java.util.Hashtable;

import static com.hazelcast.jmx.ManagementService.quote;

@ManagedDescription("HazelcastInstance.Node")
public class NodeMBean extends HazelcastMBean<Node> {

    private static final int INITIAL_CAPACITY = 3;

    public NodeMBean(HazelcastInstance hazelcastInstance, Node node, ManagementService service) {
        super(node, service);

        Hashtable<String, String> properties = new Hashtable<String, String>(INITIAL_CAPACITY);
        properties.put("type", quote("HazelcastInstance.Node"));
        properties.put("name", quote("node" + node.address));
        properties.put("instance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("address")
    @ManagedDescription("Address of the node")
    public String getName() {
        return managedObject.address.toString();
    }

    @ManagedAnnotation("masterAddress")
    @ManagedDescription("The master address of the cluster")
    public String getMasterAddress() {
        Address a = managedObject.getMasterAddress();
        return a == null ? null : a.toString();
    }
}
