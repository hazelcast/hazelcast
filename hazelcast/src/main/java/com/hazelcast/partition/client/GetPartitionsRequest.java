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

package com.hazelcast.partition.client;

import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.client.impl.client.ClientPortableHook;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;

import java.security.Permission;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public final class GetPartitionsRequest extends CallableClientRequest implements Portable, RetryableRequest {

    @Override
    public Object call() throws Exception {
        InternalPartitionService service = getService();
        service.firstArrangement();
        ClusterService clusterService = getClientEngine().getClusterService();
        Collection<MemberImpl> memberList = clusterService.getMemberList();
        Address[] addresses = new Address[memberList.size()];
        Map<Address, Integer> addressMap = new HashMap<Address, Integer>(memberList.size());
        int k = 0;
        for (MemberImpl member : memberList) {
            Address address = member.getAddress();
            addresses[k] = address;
            addressMap.put(address, k);
            k++;
        }
        InternalPartition[] partitions = service.getPartitions();
        int[] indexes = new int[partitions.length];
        for (int i = 0; i < indexes.length; i++) {
            Address owner = partitions[i].getOwnerOrNull();
            int index = -1;
            if (owner != null) {
                final Integer idx = addressMap.get(owner);
                if (idx != null) {
                    index = idx;
                }

            }
            indexes[i] = index;
        }
        return new PartitionsResponse(addresses, indexes);
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.GET_PARTITIONS;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
