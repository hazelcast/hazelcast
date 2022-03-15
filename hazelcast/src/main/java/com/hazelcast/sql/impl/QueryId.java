/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
    private long localIdHigh;

    /** Local ID: least significant bits. */
    private long localIdLow;

    public QueryId() {
        // No-op.
    }

    public QueryId(long memberIdHigh, long memberIdLow, long localIdHigh, long localIdLow) {
        this.memberIdHigh = memberIdHigh;
        this.memberIdLow = memberIdLow;
        this.localIdHigh = localIdHigh;
        this.localIdLow = localIdLow;
    }

    /**
     * Create new query ID for the given member.
     *
     * @param memberId Member ID.
     * @return Query ID.
     */
    public static QueryId create(UUID memberId) {
        UUID localId = UuidUtil.newUnsecureUUID();

        return new QueryId(
            memberId.getMostSignificantBits(),
            memberId.getLeastSignificantBits(),
            localId.getMostSignificantBits(),
            localId.getLeastSignificantBits()
        );
    }

    public static QueryId parse(String input) {
        assert input != null;

        int underscorePos = input.indexOf("_");

        if (underscorePos < 0) {
            throw new IllegalArgumentException("Query ID is malformed: " + input);
        }

        try {
            UUID memberId = UUID.fromString(input.substring(0, underscorePos));
            UUID localId = UUID.fromString(input.substring(underscorePos + 1));

            return new QueryId(
                memberId.getMostSignificantBits(),
                memberId.getLeastSignificantBits(),
                localId.getMostSignificantBits(),
                localId.getLeastSignificantBits()
            );
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Query ID is malformed: " + input, e);
        }
    }

    public UUID getMemberId() {
        return new UUID(memberIdHigh, memberIdLow);
    }

    public UUID getLocalId() {
        return new UUID(localIdHigh, localIdLow);
    }

    public long getMemberIdHigh() {
        return memberIdHigh;
    }

    public long getMemberIdLow() {
        return memberIdLow;
    }

    public long getLocalIdHigh() {
        return localIdHigh;
    }

    public long getLocalIdLow() {
        return localIdLow;
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
        out.writeLong(localIdHigh);
        out.writeLong(localIdLow);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        memberIdHigh = in.readLong();
        memberIdLow = in.readLong();
        localIdHigh = in.readLong();
        localIdLow = in.readLong();
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
            && localIdHigh == other.localIdHigh && localIdLow == other.localIdLow;
    }

    @Override
    public int hashCode() {
        int result = (int) (memberIdHigh ^ (memberIdHigh >>> 32));

        result = 31 * result + (int) (memberIdLow ^ (memberIdLow >>> 32));
        result = 31 * result + (int) (localIdHigh ^ (localIdHigh >>> 32));
        result = 31 * result + (int) (localIdLow ^ (localIdLow >>> 32));

        return result;
    }

    @Override
    public String toString() {
        return getMemberId() + "_" + getLocalId();
    }
}
