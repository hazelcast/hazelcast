/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.nio.Address;

public class PartitionInfo {
    public static int MAX_REPLICA_COUNT = 7;
    
    final int partitionId;
    final Address[] addresses = new Address[MAX_REPLICA_COUNT];

    public PartitionInfo(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Address[] getAddresses() {
        return addresses;
    }
    
    public void setReplicaAddress(int index, Address address) {
        addresses[index] = address;
    }
    
    public Address getReplicaAddress(int index) {
        return (addresses != null && addresses.length > index)
            ? addresses[index] : null;
    }
    
    public PartitionInfo copy() {
        PartitionInfo p = new PartitionInfo(partitionId);
        System.arraycopy(this.addresses, 0, p.addresses, 0, addresses.length);
        return p;
    }
}
