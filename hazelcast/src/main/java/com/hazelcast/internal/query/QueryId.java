package com.hazelcast.internal.query;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.UUID;

/**
 * Cluster-wide unique query ID.
 */
public class QueryId implements DataSerializable {
    /** Member ID. */
    private String memberId;

    /** Local ID: most significant bits. */
    private long localHigh;

    /** Local ID: least significant bits. */
    private long localLow;

    public QueryId() {
    }

    public QueryId(String memberId, long localHigh, long localLow) {
        this.memberId = memberId;
        this.localHigh = localHigh;
        this.localLow = localLow;
    }

    public String getMemberId() {
        return memberId;
    }

    public UUID getLocalId() {
        return new UUID(localHigh, localLow);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(memberId);
        out.writeLong(localHigh);
        out.writeLong(localLow);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        memberId = in.readUTF();
        localHigh = in.readLong();
        localLow = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryId other = (QueryId)o;

        return localHigh == other.localHigh && localLow == other.localLow &&
            memberId != null ? memberId.equals(other.memberId) : other.memberId == null;
    }

    @Override
    public int hashCode() {
        int result = memberId != null ? memberId.hashCode() : 0;
        result = 31 * result + (int) (localHigh ^ (localHigh >>> 32));
        result = 31 * result + (int) (localLow ^ (localLow >>> 32));

        return result;
    }

    @Override
    public String toString() {
        return "QueryId {memberId=" + memberId + ", id=" + new UUID(localHigh, localLow) + '}';
    }
}
