/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;
import java.net.InetSocketAddress;

import static com.hazelcast.util.JsonUtil.getInt;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.PartitionServiceMBean}.
 */
public class SerializablePartitionServiceBean implements JsonSerializable {

    private int partitionCount;
    private int activePartitionCount;

    public SerializablePartitionServiceBean() {
    }

    public SerializablePartitionServiceBean(InternalPartitionService partitionService,
                                            HazelcastInstanceImpl hazelcastInstance) {
        InetSocketAddress address = hazelcastInstance.getCluster().getLocalMember().getSocketAddress();
        this.partitionCount = partitionService.getPartitionCount();
        this.activePartitionCount = partitionService.getMemberPartitions(new Address(address)).size();
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
