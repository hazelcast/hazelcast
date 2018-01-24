package com.hazelcast.raft.impl.testing;

import com.hazelcast.raft.RaftGroupId;

public class TestRaftGroupId implements RaftGroupId {

    private String name;

    public TestRaftGroupId(String name) {
        assert name != null;
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long commitIndex() {
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TestRaftGroupId)) {
            return false;
        }

        TestRaftGroupId that = (TestRaftGroupId) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return "TestRaftGroupId{" + "name='" + name + "\''}";
    }


}
