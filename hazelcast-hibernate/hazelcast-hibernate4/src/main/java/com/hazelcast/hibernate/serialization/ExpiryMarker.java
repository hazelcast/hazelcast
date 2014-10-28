package com.hazelcast.hibernate.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.hibernate.cache.spi.access.SoftLock;

import java.io.IOException;
import java.util.Comparator;

/**
 * An entry which is marked for expiration. This can occur when Hibernate is expecting to update an entry as a result
 * of changes being made in an in-progress transaction
 * <p/>
 * Such an entry has the following properties
 * <ul>
 *     <li>It will always return a null value, resulting in a cache miss</li>
 *     <li>It is only replaceable when it is completely expired</li>
 *     <li>It can be marked by multiple transactions at the same time and will not expire until all transactions complete</li>
 *     <li>It should not be expired unless {@link #matches(SoftLock)} is true</li>
 * </ul>
 */
public class ExpiryMarker extends Expirable implements SoftLock {

    private static final long NOT_COMPLETELY_EXPIRED = -1;

    private boolean concurrent;
    private long expiredTimestamp;
    private String markerId;
    private int multiplicity;
    private long timeout;

    public ExpiryMarker() {
    }

    public ExpiryMarker(Object version, long timeout, String markerId) {
        this(version, false, NOT_COMPLETELY_EXPIRED, markerId, 1, timeout);
    }

    private ExpiryMarker(Object version, boolean concurrent, long expiredTimestamp, String markerId, int multiplicity,
                         long timeout) {
        super(version);
        this.concurrent = concurrent;
        this.expiredTimestamp = expiredTimestamp;
        this.markerId = markerId;
        this.multiplicity = multiplicity;
        this.timeout = timeout;
    }

    @Override
    public boolean isReplaceableBy(long txTimestamp, Object newVersion, Comparator versionComparator) {
        // If the marker has timed out it should be fine to write to write to it
        if (txTimestamp > timeout) {
            return true;
        }

        // If the marker is still marked, it is definitely not replaceable
        if (multiplicity > 0) {
            return false;
        }

        if (version == null) {
            // If the marker was expired in the past, its good to write to
            return expiredTimestamp != NOT_COMPLETELY_EXPIRED && txTimestamp > expiredTimestamp;
        }

        //noinspection unchecked
        return versionComparator.compare(version, newVersion) < 0;
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public Object getValue(long txTimestamp) {
        return null;
    }

    @Override
    public boolean matches(SoftLock lock) {
        if (lock instanceof ExpiryMarker) {
            ExpiryMarker other = (ExpiryMarker) lock;
            return markerId.equals(other.markerId);
        }

        return false;
    }

    /**
     * @return {@code true} if the marker has ever been {@link #markForExpiration(long, String)}
     *         concurrently, {@code false} otherwise
     */
    public boolean isConcurrent() {
        return concurrent;
    }

    @Override
    public ExpiryMarker markForExpiration(long timeout, String nextMarkerId) {
        return new ExpiryMarker(version, true, NOT_COMPLETELY_EXPIRED, markerId, multiplicity + 1, timeout);
    }

    /**
     * Expire the marker. The marker may have been marked multiple times so it may still not
     * be {@link #isReplaceableBy(long, Object, Comparator) replaceable}.
     *
     * @param timestamp the timestamp to specify when it was completely expired
     * @return a new {@link ExpiryMarker}
     */
    public ExpiryMarker expire(long timestamp) {
        int newMultiplicity = multiplicity - 1;
        long newExpiredTimestamp = newMultiplicity == 0 ? timestamp : expiredTimestamp;

        return new ExpiryMarker(version, concurrent, newExpiredTimestamp, markerId, newMultiplicity, timeout);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeBoolean(concurrent);
        out.writeUTF(markerId);
        out.writeInt(multiplicity);
        out.writeLong(timeout);
        out.writeLong(expiredTimestamp);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        concurrent = in.readBoolean();
        markerId = in.readUTF();
        multiplicity = in.readInt();
        timeout = in.readLong();
        expiredTimestamp = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return HibernateDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return HibernateDataSerializerHook.EXPIRY_MARKER;
    }

}
