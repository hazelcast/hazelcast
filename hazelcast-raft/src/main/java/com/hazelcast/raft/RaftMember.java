package com.hazelcast.raft;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftMember {

    private String address;
    private String id;

    public RaftMember() {
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

        if (address != null ? !address.equals(that.address) : that.address != null) {
            return false;
        }
        return id != null ? id.equals(that.id) : that.id == null;
    }

    @Override
    public int hashCode() {
        int result = address != null ? address.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "RaftMember{" + "address='" + address + '\'' + ", id='" + id + '\'' + '}';
    }
}
