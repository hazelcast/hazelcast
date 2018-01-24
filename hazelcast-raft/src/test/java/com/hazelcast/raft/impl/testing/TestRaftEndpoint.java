package com.hazelcast.raft.impl.testing;

import com.hazelcast.raft.impl.RaftEndpoint;

public class TestRaftEndpoint implements RaftEndpoint {

    private String uuid;

    private int port;

    public TestRaftEndpoint(String uuid, int port) {
        this.uuid = uuid;
        this.port = port;
    }

    @Override
    public String getUid() {
        return uuid;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TestRaftEndpoint that = (TestRaftEndpoint) o;

        if (port != that.port) {
            return false;
        }
        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        int result = uuid.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return "TestRaftEndpoint{" + "uuid='" + uuid + '\'' + ", port=" + port + '}';
    }

}
