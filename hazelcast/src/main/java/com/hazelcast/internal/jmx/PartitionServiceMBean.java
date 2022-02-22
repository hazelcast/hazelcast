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

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.cluster.Address;
import com.hazelcast.partition.PartitionService;

import java.util.Map;

import static com.hazelcast.internal.jmx.ManagementService.quote;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Management bean for {@link PartitionService}
 */
@ManagedDescription("HazelcastInstance.PartitionServiceMBean")
public class PartitionServiceMBean extends HazelcastMBean<InternalPartitionService> {

    private static final int INITIAL_CAPACITY = 3;
    private final HazelcastInstanceImpl hazelcastInstance;

    public PartitionServiceMBean(HazelcastInstanceImpl hazelcastInstance, InternalPartitionService partitionService,
                                 ManagementService service) {
        super(partitionService, service);

        this.hazelcastInstance = hazelcastInstance;
        final Map<String, String> properties = createHashMap(INITIAL_CAPACITY);
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
        Address thisAddress = hazelcastInstance.getCluster().getLocalMember().getAddress();
        return managedObject.getMemberPartitionsIfAssigned(thisAddress).size();
    }

    @ManagedAnnotation("isClusterSafe")
    @ManagedDescription("Is the cluster in a safe state")
    public boolean isClusterSafe() {
        return hazelcastInstance.getPartitionService().isClusterSafe();
    }

    @ManagedAnnotation("isLocalMemberSafe")
    @ManagedDescription("Is the local member safe to shutdown")
    public boolean isLocalMemberSafe() {
        return hazelcastInstance.getPartitionService().isLocalMemberSafe();
    }
}
