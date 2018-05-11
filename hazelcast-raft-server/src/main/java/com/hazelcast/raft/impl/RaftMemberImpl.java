package com.hazelcast.raft.impl;

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * {@code RaftMember} represents a member in Raft group.
 * Each member must have a unique address and id in the group.
 */
public class RaftMemberImpl implements RaftMember, Serializable, IdentifiedDataSerializable {

    private static final long serialVersionUID = 5628148969327743953L;

    private transient String uid;
    private transient Address address;

    public RaftMemberImpl() {
    }

    public RaftMemberImpl(String id, Address address) {
        this.uid = id;
        this.address = address;
    }

    public RaftMemberImpl(Member member) {
        this.uid = member.getUuid();
        this.address = member.getAddress();
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
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.MEMBER;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RaftMemberImpl)) {
            return false;
        }

        RaftMemberImpl that = (RaftMemberImpl) o;

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
        return "RaftMember{" + "uid=" + uid + ", address=" + address + '}';
    }
}
