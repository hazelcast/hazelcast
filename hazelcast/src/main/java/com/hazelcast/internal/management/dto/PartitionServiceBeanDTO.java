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

package com.hazelcast.internal.management.dto;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.json.JsonSerializable;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.cluster.Address;

import static com.hazelcast.internal.util.JsonUtil.getInt;

/**
 * A Serializable DTO for {@link com.hazelcast.internal.jmx.PartitionServiceMBean}.
 */
public class PartitionServiceBeanDTO implements JsonSerializable {

    private int partitionCount;
    private int activePartitionCount;

    public PartitionServiceBeanDTO() {
    }

    public PartitionServiceBeanDTO(InternalPartitionService partitionService,
                                   HazelcastInstanceImpl hazelcastInstance) {
        Address address = hazelcastInstance.getCluster().getLocalMember().getAddress();
        this.partitionCount = partitionService.getPartitionCount();
        this.activePartitionCount = partitionService.getMemberPartitionsIfAssigned(address).size();
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public int getActivePartitionCount() {
        return activePartitionCount;
    }

    public void setActivePartitionCount(int activePartitionCount) {
        this.activePartitionCount = activePartitionCount;
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("partitionCount", partitionCount);
        root.add("activePartitionCount", activePartitionCount);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        partitionCount = getInt(json, "partitionCount", -1);
        activePartitionCount = getInt(json, "activePartitionCount", -1);
    }
}
