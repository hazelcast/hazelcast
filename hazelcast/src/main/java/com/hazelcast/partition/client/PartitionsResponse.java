package com.hazelcast.partition.client;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionDataSerializerHook;

import java.io.IOException;

/**
 * @mdogan 5/16/13
 */
public final class PartitionsResponse implements IdentifiedDataSerializable {

    private Address[] members;

    private int[] ownerIndexes;

    public PartitionsResponse() {
    }

    public PartitionsResponse(Address[] members, int[] ownerIndexes) {
        this.members = members;
        this.ownerIndexes = ownerIndexes;
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
        len =  in.readInt();
        ownerIndexes = new int[len];
        for (int i = 0; i < len; i++) {
            ownerIndexes[i] = in.readInt();
        }
    }

    public Address[] getMembers() {
        return members;
    }

    public int[] getOwnerIndexes() {
        return ownerIndexes;
    }
}
