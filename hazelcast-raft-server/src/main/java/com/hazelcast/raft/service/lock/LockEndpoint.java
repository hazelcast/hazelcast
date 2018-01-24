package com.hazelcast.raft.service.lock;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LockEndpoint {
    public final String uid;
    public final long threadId;

    public LockEndpoint(String uid, long threadId) {
        this.uid = uid;
        this.threadId = threadId;
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
        return threadId == that.threadId && uid.equals(that.uid);
    }

    @Override
    public int hashCode() {
        int result = uid.hashCode();
        result = 31 * result + (int) (threadId ^ (threadId >>> 32));
        return result;
    }
}
