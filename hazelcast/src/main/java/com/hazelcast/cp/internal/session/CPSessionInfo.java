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

package com.hazelcast.cp.internal.session;

import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.session.CPSession;
import com.hazelcast.internal.util.Clock;

import java.io.IOException;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static java.lang.Math.max;

/**
 * Represents state of a CP session.
 */
public class CPSessionInfo implements CPSession, IdentifiedDataSerializable {

    private long id;

    private long version;

    private Address endpoint;

    private String endpointName;

    private CPSessionOwnerType endpointType;

    private long creationTime;

    private long expirationTime;

    public CPSessionInfo() {
    }

    CPSessionInfo(long id, long version, Address endpoint, String endpointName, CPSessionOwnerType endpointType,
                  long creationTime, long expirationTime) {
        checkTrue(version >= 0, "Session: " + id + " cannot have a negative version: " + version);
        this.id = id;
        this.version = version;
        this.endpoint = endpoint;
        this.endpointName = endpointName;
        this.endpointType = endpointType;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public long expirationTime() {
        return expirationTime;
    }

    @Override
    public boolean isExpired(long timestamp) {
        return expirationTime() <= timestamp;
    }

    @Override
    public long version() {
        return version;
    }

    @Override
    public Address endpoint() {
        return endpoint;
    }

    @Override
    public CPSessionOwnerType endpointType() {
        return endpointType;
    }

    @Override
    public String endpointName() {
        return endpointName;
    }

    CPSessionInfo heartbeat(long ttlMs) {
        long newExpirationTime = max(expirationTime, toExpirationTime(Clock.currentTimeMillis(), ttlMs));
        return newSession(newExpirationTime);
    }

    CPSessionInfo shiftExpirationTime(long durationMs) {
        long newExpirationTime = toExpirationTime(expirationTime, durationMs);
        return newSession(newExpirationTime);
    }

    private CPSessionInfo newSession(long newExpirationTime) {
        return new CPSessionInfo(id, version + 1, endpoint, endpointName, endpointType, creationTime, newExpirationTime);
    }

    static long toExpirationTime(long timestamp, long ttlMillis) {
        long expirationTime = timestamp + ttlMillis;
        return expirationTime > 0 ? expirationTime : Long.MAX_VALUE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CPSessionInfo that = (CPSessionInfo) o;
        return id == that.id && version == that.version;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "CPSessionInfo{" + "id=" + id + ", version=" + version + ", endpoint=" + endpoint + ", endpointName='"
                + endpointName + '\'' + ", endpointType=" + endpointType + ", creationTime=" + creationTime + ", expirationTime="
                + expirationTime + '}';
    }

    @Override
    public int getFactoryId() {
        return RaftSessionServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftSessionServiceDataSerializerHook.RAFT_SESSION;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(creationTime);
        out.writeLong(expirationTime);
        out.writeLong(version);
        out.writeObject(endpoint);
        boolean containsEndpointName = (endpointName != null);
        out.writeBoolean(containsEndpointName);
        if (containsEndpointName) {
            out.writeString(endpointName);
        }
        out.writeString(endpointType.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readLong();
        creationTime = in.readLong();
        expirationTime = in.readLong();
        version = in.readLong();
        endpoint = in.readObject();
        boolean containsEndpointName = in.readBoolean();
        if (containsEndpointName) {
            endpointName = in.readString();
        }
        endpointType = CPSessionOwnerType.valueOf(in.readString());
    }
}
