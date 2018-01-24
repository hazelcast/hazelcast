package com.hazelcast.raft.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.config.raft.RaftMember;
import com.hazelcast.util.AddressUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code RaftEndpoint} represents a member in Raft group.
 * Each endpoint must have a unique address and id in the group.
 */
public class RaftEndpointImpl implements RaftEndpoint, Serializable {

    private static final long serialVersionUID = 5628148969327743953L;

    public static List<RaftEndpointImpl> parseEndpoints(List<RaftMember> members) throws UnknownHostException {
        List<RaftEndpointImpl> endpoints = new ArrayList<RaftEndpointImpl>(members.size());
        for (RaftMember member : members) {
            AddressUtil.AddressHolder addressHolder = AddressUtil.getAddressHolder(member.getAddress());
            Address address = new Address(addressHolder.getAddress(), addressHolder.getPort());
            address.setScopeId(addressHolder.getScopeId());
            endpoints.add(new RaftEndpointImpl(member.getUid(), address));
        }
        return endpoints;
    }

    private transient String uid;
    private transient Address address;

    public RaftEndpointImpl() {
    }

    public RaftEndpointImpl(String id, Address address) {
        this.uid = id;
        this.address = address;
    }

    public String getUid() {
        return uid;
    }

    public Address getAddress() {
        return address;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(uid);
        out.writeUTF(address.getHost());
        out.writeInt(address.getPort());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        uid = in.readUTF();
        String host = in.readUTF();
        int port = in.readInt();
        address = new Address(host, port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RaftEndpointImpl)) {
            return false;
        }

        RaftEndpointImpl that = (RaftEndpointImpl) o;

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
