package com.hazelcast.hibernate.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.hibernate.cache.spi.access.SoftLock;

import java.io.IOException;
import java.util.Comparator;

/**
 * A container class which represents an entry in a region cache which can be marked for expiration
 */
public abstract class Expirable implements IdentifiedDataSerializable {

    protected Object version;

    protected Expirable() {
    }

    protected Expirable(Object version) {
        this.version = version;
    }

    /**
     * Determine if the current entry can be overridden with a value corresponding to the given new version
     * and the transaction timestamp.
     *
     * @param txTimestamp       the timestamp of the transaction
     * @param newVersion        the new version for the replacement value
     * @param versionComparator the comparator to use for the version
     * @return {@code true} if the value can be replaced, {@code false} otherwise
     */
    public abstract boolean isReplaceableBy(long txTimestamp, Object newVersion, Comparator versionComparator);

    /**
     * @return the value contained, or {@code null} if none exists
     */
    public abstract Object getValue();

    /**
     * @param txTimestamp the timestamp of the transaction
     * @return the value contained if it was created before the transaction timestamp or {@code null}
     */
    public abstract Object getValue(long txTimestamp);

    /**
     * @return the version representing the value of {@code null} if the entry is not versioned
     */
    public Object getVersion() {
        return version;
    }

    /**
     * @return {@code true} if the {@link Expirable} matches using the specified lock, {@code false} otherwise
     * @see ExpiryMarker#expire(long)
     */
    public abstract boolean matches(SoftLock lock);

    /**
     * Mark the entry for expiration with the given timeout and marker id.
     * <p/>
     * For every invocation a corresponding call to {@link ExpiryMarker#expire(long)} should be made, provided that
     * the returned marker {@link #matches(SoftLock) matches}
     *
     * @param timeout      the timestamp in which the lock times out
     * @param nextMarkerId the next lock id to use if creating a new lock
     * @return the newly created marker, or the current marker with a higher multiplicity
     * @see ExpiryMarker#expire(long)
     */
    public abstract ExpiryMarker markForExpiration(long timeout, String nextMarkerId);

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(version);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        version = in.readObject();
    }

}
