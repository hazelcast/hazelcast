/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

/**
 * Cluster-wide unique query ID.
 */
public final class QueryId implements IdentifiedDataSerializable {
    /** Member ID: most significant bits */
    private long memberIdHigh;

    /** Member ID: least significant bits. */
    private long memberIdLow;

    /** Local ID: most significant bits. */
    private long localHigh;

    /** Local ID: least significant bits. */
    private long localLow;

    public QueryId() {
        // No-op.
    }

    QueryId(long memberIdHigh, long memberIdLow, long localHigh, long localLow) {
        this.memberIdHigh = memberIdHigh;
        this.memberIdLow = memberIdLow;
        this.localHigh = localHigh;
        this.localLow = localLow;
    }

    /**
     * Create new query ID for the given member.
     *
     * @param memberId Member ID.
     * @return Query ID.
     */
    public static QueryId create(UUID memberId) {
        UUID qryId = UuidUtil.newUnsecureUUID();

        return new QueryId(
            memberId.getMostSignificantBits(),
            memberId.getLeastSignificantBits(),
            qryId.getMostSignificantBits(),
            qryId.getLeastSignificantBits()
        );
    }

    public UUID getMemberId() {
        return new UUID(memberIdHigh, memberIdLow);
    }

    public UUID getLocalId() {
        return new UUID(localHigh, localLow);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.QUERY_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(memberIdHigh);
        out.writeLong(memberIdLow);
        out.writeLong(localHigh);
        out.writeLong(localLow);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        memberIdHigh = in.readLong();
        memberIdLow = in.readLong();
        localHigh = in.readLong();
        localLow = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QueryId other = (QueryId) o;

        return memberIdHigh == other.memberIdHigh && memberIdLow == other.memberIdLow
            && localHigh == other.localHigh && localLow == other.localLow;
    }

    @Override
    public int hashCode() {
        int result = (int) (memberIdHigh ^ (memberIdHigh >>> 32));

        result = 31 * result + (int) (memberIdLow ^ (memberIdLow >>> 32));
        result = 31 * result + (int) (localHigh ^ (localHigh >>> 32));
        result = 31 * result + (int) (localLow ^ (localLow >>> 32));

        return result;
    }

    @Override
    public String toString() {
        return "QueryId {memberId=" + getMemberId() + ", localId=" + getLocalId() + '}';
    }
}
