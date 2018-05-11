package com.hazelcast.raft.service.lock;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LockEndpoint implements IdentifiedDataSerializable {
    private long sessionId;
    private long threadId;

    public LockEndpoint() {
    }

    public LockEndpoint(long sessionId, long threadId) {
        this.sessionId = sessionId;
        this.threadId = threadId;
    }

    public long sessionId() {
        return sessionId;
    }

    public long threadId() {
        return threadId;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftLockDataSerializerHook.LOCK_ENDPOINT;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeLong(sessionId);
        out.writeLong(threadId);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        sessionId = in.readLong();
        threadId = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LockEndpoint)) {
            return false;
        }

        LockEndpoint that = (LockEndpoint) o;

        if (sessionId != that.sessionId) {
            return false;
        }
        return threadId == that.threadId;
    }

    @Override
    public int hashCode() {
        int result = (int) (sessionId ^ (sessionId >>> 32));
        result = 31 * result + (int) (threadId ^ (threadId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LockEndpoint{" + "sessionId=" + sessionId + ", threadId=" + threadId + '}';
    }
}
