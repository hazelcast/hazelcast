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

package com.hazelcast.internal.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.cluster.Address;

import java.util.Map;

import static com.hazelcast.internal.jmx.ManagementService.quote;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Management bean for {@link Node}
 */
@ManagedDescription("HazelcastInstance.Node")
public class NodeMBean extends HazelcastMBean<Node> {

    private static final int INITIAL_CAPACITY = 3;

    public NodeMBean(HazelcastInstance hazelcastInstance, Node node, ManagementService service) {
        super(node, service);

        final Map<String, String> properties = createHashMap(INITIAL_CAPACITY);
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
        Address masterAddress = managedObject.getMasterAddress();
        return masterAddress == null ? null : masterAddress.toString();
    }
}
