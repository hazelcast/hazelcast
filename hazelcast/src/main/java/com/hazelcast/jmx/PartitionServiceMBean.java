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

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;

import java.net.InetSocketAddress;
import java.util.Hashtable;

import static com.hazelcast.jmx.ManagementService.quote;

@ManagedDescription("HazelcastInstance.PartitionServiceMBean")
public class PartitionServiceMBean  extends HazelcastMBean<InternalPartitionService> {

    private final HazelcastInstanceImpl hazelcastInstance;

    public PartitionServiceMBean(HazelcastInstanceImpl hazelcastInstance, InternalPartitionService partitionService,
                                 ManagementService service) {
        super(partitionService, service);

        this.hazelcastInstance = hazelcastInstance;
        Hashtable<String, String> properties = new Hashtable<String, String>(3);
        properties.put("type", quote("HazelcastInstance.PartitionServiceMBean"));
        properties.put("name", quote(hazelcastInstance.getName()));
        properties.put("instance", quote(hazelcastInstance.getName()));

        setObjectName(properties);
    }

    @ManagedAnnotation("partitionCount")
    @ManagedDescription("Number of partitions")
    public int getPartitionCount() {
        return managedObject.getPartitionCount();
    }

    @ManagedAnnotation("activePartitionCount")
    @ManagedDescription("Number of active partitions")
    public int getActivePartitionCount() {
        InetSocketAddress address = hazelcastInstance.getCluster().getLocalMember().getSocketAddress();
        return managedObject.getMemberPartitions(new Address(address)).size();
    }
}
