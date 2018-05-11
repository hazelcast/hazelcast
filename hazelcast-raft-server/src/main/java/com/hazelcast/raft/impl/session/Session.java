/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.raft.SessionInfo;
import com.hazelcast.util.Clock;

import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.Math.max;

/**
 * TODO: Javadoc Pending...
 */
public class Session implements SessionInfo {

    private final long id;

    private final long creationTime;

    private final long expirationTime;

    private final long version;

    // used for diagnostics
    private final Address endpoint;

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
}
