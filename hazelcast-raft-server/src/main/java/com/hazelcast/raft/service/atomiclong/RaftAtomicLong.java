package com.hazelcast.raft.service.atomiclong;

import com.hazelcast.raft.RaftGroupId;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftAtomicLong {

    private final RaftGroupId groupId;
    private final String name;

    private long value;

    RaftAtomicLong(RaftGroupId groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    RaftAtomicLong(RaftGroupId groupId, String name, long value) {
        this.groupId = groupId;
        this.name = name;
        this.value = value;
    }

    public RaftGroupId groupId() {
        return groupId;
    }

    public String name() {
        return name;
    }

    public long addAndGet(long delta) {
        return value += delta;
    }

    public long getAndAdd(long delta) {
        long v = value;
        value += delta;
        return v;
    }

    public long getAndSet(long value) {
        long v = this.value;
        this.value = value;
        return v;
    }

    public boolean compareAndSet(long currentValue, long newValue) {
        if (value == currentValue) {
            value = newValue;
            return true;
        }
        return false;
    }

    public long value() {
        return value;
    }

    @Override
    public String toString() {
        return "RaftAtomicLong{" + "groupId=" + groupId + ", name='" + name + '\'' + ", value=" + value + '}';
    }
}
