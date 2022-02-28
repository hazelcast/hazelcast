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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Represents information of a session that is just created.
 * {@link #ttlMillis} and {@link #heartbeatMillis} must be respected
 * by the caller.
 */
public class SessionResponse implements IdentifiedDataSerializable {

    private long sessionId;

    private long ttlMillis;

    private long heartbeatMillis;

    SessionResponse() {
    }

    public SessionResponse(long sessionId, long ttlMillis, long heartbeatMillis) {
        this.sessionId = sessionId;
        this.ttlMillis = ttlMillis;
        this.heartbeatMillis = heartbeatMillis;
    }

    public long getSessionId() {
        return sessionId;
    }

    public long getTtlMillis() {
        return ttlMillis;
    }

    public long getHeartbeatMillis() {
        return heartbeatMillis;
    }

    @Override
    public int getFactoryId() {
        return RaftSessionServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftSessionServiceDataSerializerHook.SESSION_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(sessionId);
        out.writeLong(ttlMillis);
        out.writeLong(heartbeatMillis);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        sessionId = in.readLong();
        ttlMillis = in.readLong();
        heartbeatMillis = in.readLong();
    }

    @Override
    public String toString() {
        return "SessionResponse{" + "sessionId=" + sessionId + ", ttlMillis=" + ttlMillis + ", heartbeatMillis="
                + heartbeatMillis + '}';
    }
}
