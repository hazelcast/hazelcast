package com.hazelcast.raft.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.util.AddressUtil;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code RaftEndpoint} represents a member in Raft group.
 * Each endpoint must have a unique address and id in the group.
 */
public class RaftEndpoint implements IdentifiedDataSerializable {

    public static List<RaftEndpoint> parseEndpoints(List<RaftMember> members) throws UnknownHostException {
        List<RaftEndpoint> endpoints = new ArrayList<RaftEndpoint>(members.size());
        for (RaftMember member : members) {
            AddressUtil.AddressHolder addressHolder = AddressUtil.getAddressHolder(member.getAddress());
            Address address = new Address(addressHolder.getAddress(), addressHolder.getPort());
            address.setScopeId(addressHolder.getScopeId());
            endpoints.add(new RaftEndpoint(member.getId(), address));
        }
        return endpoints;
    }

    private String uid;
    private Address address;

    public RaftEndpoint() {
    }

    public RaftEndpoint(String id, Address address) {
        this.uid = id;
        this.address = address;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getUid() {
        return uid;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.ENDPOINT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(uid);
        out.writeObject(address);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        uid = in.readUTF();
        address = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RaftEndpoint)) {
            return false;
        }

        RaftEndpoint that = (RaftEndpoint) o;

        if (uid != null ? !uid.equals(that.uid) : that.uid != null) {
            return false;
        }
        return address != null ? address.equals(that.address) : that.address == null;
    }

    @Override
    public int hashCode() {
        int result = uid != null ? uid.hashCode() : 0;
        result = 31 * result + (address != null ? address.hashCode() : 0);
        return result;
    }


    @Override
    public String toString() {
        return "RaftEndpoint{" + "uid=" + uid + ", address=" + address + '}';
    }
}
