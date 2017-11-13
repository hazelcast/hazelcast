package com.hazelcast.raft;

/**
 * Represents a member of the Raft group.
 * Each member must have a unique address and id in the group.
 */
public class RaftMember {

    private String address;
    private String id;

    public RaftMember() {
    }

    public RaftMember(RaftMember member) {
        this(member.address, member.id);
    }

    public RaftMember(String address, String id) {
        this.address = address;
        this.id = id;
    }

    public String getAddress() {
        return address;
    }

    public RaftMember setAddress(String address) {
        this.address = address;
        return this;
    }

    public String getId() {
        return id;
    }

    public RaftMember setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RaftMember)) {
            return false;
        }

        RaftMember that = (RaftMember) o;
        return address.equals(that.address) && id.equals(that.id);
    }

    @Override
    public int hashCode() {
        int result = address.hashCode();
        result = 31 * result + id.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "RaftMember{" + "address='" + address + '\'' + ", id='" + id + '\'' + '}';
    }
}
