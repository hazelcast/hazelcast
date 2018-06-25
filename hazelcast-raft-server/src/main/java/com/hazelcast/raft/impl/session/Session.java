/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.impl.session;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.SessionInfo;
import com.hazelcast.util.Clock;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.Math.max;

/**
 * Represents state of a Raft session.
 */
public class Session implements SessionInfo, IdentifiedDataSerializable {

    private long id;

    private long creationTime;

    private long expirationTime;

    private long version;

    // used for diagnostics
    private Address endpoint;

    public Session() {
    }

    Session(long id, long creationTime, long expirationTime, Address endpoint) {
        this(id, creationTime, expirationTime, 0, endpoint);
    }

    Session(long id, long creationTime, long expirationTime, long version, Address endpoint) {
        checkTrue(version >= 0, "Session: " + id + " cannot have a negative version: " + version);
        this.id = id;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
        this.version = version;
        this.endpoint = endpoint;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Session session = (Session) o;

        if (id != session.id) {
            return false;
        }
        if (version != session.version) {
            return false;
        }
        return endpoint != null ? endpoint.equals(session.endpoint) : session.endpoint == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + (endpoint != null ? endpoint.hashCode() : 0);
        return result;
    }

    Session heartbeat(long ttlMs) {
        long newExpirationTime = max(expirationTime, toExpirationTime(Clock.currentTimeMillis(), ttlMs));
        return newSession(newExpirationTime);
    }

    Session shiftExpirationTime(long durationMs) {
        long newExpirationTime = toExpirationTime(expirationTime, durationMs);
        return newSession(newExpirationTime);
    }

    private Session newSession(long newExpirationTime) {
        return new Session(id, creationTime, newExpirationTime, version + 1, endpoint);
    }

    static long toExpirationTime(long timestamp, long ttlMillis) {
        long expirationTime = timestamp + ttlMillis;
        return expirationTime > 0 ? expirationTime : Long.MAX_VALUE;
    }

    @Override
    public String toString() {
        return "Session{" + "id=" + id + ", creationTime=" + creationTime + ", expirationTime=" + expirationTime
                + ", version=" + version + ", endpoint=" + endpoint + '}';
    }

    @Override
    public int getFactoryId() {
        return RaftSessionServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSessionServiceDataSerializerHook.SESSION;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(creationTime);
        out.writeLong(expirationTime);
        out.writeLong(version);
        out.writeObject(endpoint);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readLong();
        creationTime = in.readLong();
        expirationTime = in.readLong();
        version = in.readLong();
        endpoint = in.readObject();
    }
}
