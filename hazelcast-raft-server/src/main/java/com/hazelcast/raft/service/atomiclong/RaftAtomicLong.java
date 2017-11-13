package com.hazelcast.raft.service.atomiclong;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftAtomicLong {

    private final String name;

    private long value;
    private long commitIndex;

    RaftAtomicLong(String name) {
        this.name = name;
    }

    RaftAtomicLong(String name, long value, long commitIndex) {
        this.name = name;
        this.value = value;
        this.commitIndex = commitIndex;
    }

    public long addAndGet(long delta, long commitIndex) {
        this.commitIndex = commitIndex;
        return value += delta;
    }

    public long getAndAdd(long delta, long commitIndex) {
        this.commitIndex = commitIndex;
        long v = value;
        value += delta;
        return v;
    }

    public long getAndSet(long value, long commitIndex) {
        this.commitIndex = commitIndex;
        long v = this.value;
        this.value = value;
        return v;
    }

    public boolean compareAndSet(long currentValue, long newValue, long commitIndex) {
        this.commitIndex = commitIndex;
        if (value == currentValue) {
            value = newValue;
            return true;
        }
        return false;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long value() {
        return value;
    }

    @Override
    public String toString() {
        return "AtomicLong{" + "name='" + name + '\'' + ", value=" + value + ", commitIndex=" + commitIndex + '}';
    }
}
