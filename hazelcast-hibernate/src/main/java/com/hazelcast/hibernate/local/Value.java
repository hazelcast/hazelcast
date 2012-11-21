package com.hazelcast.hibernate.local;

import org.hibernate.cache.access.SoftLock;

/**
 * @mdogan 11/12/12
 */
public class Value {

    private final Object version;

    private final Object value;

    private final SoftLock lock;

    private final long creationTime;

    public Value(final Object version, final Object value, final long creationTime) {
        this.version = version;
        this.value = value;
        this.creationTime = creationTime;
        this.lock = null;
    }

    public Value(final Object version, final Object value, final SoftLock lock, final long creationTime) {
        this.version = version;
        this.value = value;
        this.lock = lock;
        this.creationTime = creationTime;
    }

    public Object getValue() {
        return value;
    }

    public Object getVersion() {
        return version;
    }

    public SoftLock getLock() {
        return lock;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public Value createLockedValue(SoftLock lock) {
        return new Value(version, value, lock, creationTime);
    }

    public Value createUnlockedValue() {
        return new Value(version, value, null, creationTime);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Value value1 = (Value) o;

        if (value != null ? !value.equals(value1.value) : value1.value != null) return false;
        if (version != null ? !version.equals(value1.version) : value1.version != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = version != null ? version.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Value");
        sb.append("{value=").append(value);
        sb.append(", version=").append(version);
        sb.append('}');
        return sb.toString();
    }

}

