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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionDataSerializerHook;

import java.io.IOException;

//This is an internal structure, so doesn't matter if we expose arrays.
public final class PartitionsResponse implements IdentifiedDataSerializable {

    private Address[] members;
    private int[] ownerIndexes;

    public PartitionsResponse() {
    }

    public PartitionsResponse(Address[] members, int[] ownerIndexes) {
        this.members = members;
        this.ownerIndexes = ownerIndexes;
    }

    public Address[] getMembers() {
        return members;
    }

    public int[] getOwnerIndexes() {
        return ownerIndexes;
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.PARTITIONS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(members.length);
        for (Address member : members) {
            member.writeData(out);
        }
        out.writeInt(ownerIndexes.length);
        for (int index : ownerIndexes) {
            out.writeInt(index);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        members = new Address[len];
        for (int i = 0; i < len; i++) {
            Address a = new Address();
            a.readData(in);
            members[i] = a;
        }
        len = in.readInt();
        ownerIndexes = new int[len];
        for (int i = 0; i < len; i++) {
            ownerIndexes[i] = in.readInt();
        }
    }
}
