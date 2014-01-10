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

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientPortableHook;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.PartitionServiceImpl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mdogan 5/13/13
 */
public final class GetPartitionsRequest extends CallableClientRequest implements Portable, RetryableRequest {

    public Object call() throws Exception {
        final PartitionServiceImpl service = getService();
        service.firstArrangement();
        final ClusterService clusterService = getClientEngine().getClusterService();
        final Collection<MemberImpl> memberList = clusterService.getMemberList();
        final Address[] addresses = new Address[memberList.size()];
        final Map<Address, Integer> addressMap = new HashMap<Address, Integer>(memberList.size());
        int k = 0;
        for (MemberImpl member : memberList) {
            final Address address = member.getAddress();
            addresses[k] = address;
            addressMap.put(address, k);
            k++;
        }
        final InternalPartition[] partitions = service.getPartitions();
        final int[] indexes = new int[partitions.length];
        for (int i = 0; i < indexes.length; i++) {
            final Address owner = partitions[i].getOwner();
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

    public String getServiceName() {
        return PartitionServiceImpl.SERVICE_NAME;
    }

    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    public int getClassId() {
        return ClientPortableHook.GET_PARTITIONS;
    }

}
