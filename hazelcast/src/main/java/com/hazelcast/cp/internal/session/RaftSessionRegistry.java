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

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.session.CPSession;
import com.hazelcast.cp.session.CPSession.CPSessionOwnerType;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.cp.internal.session.CPSessionInfo.toExpirationTime;

/**
 * Maintains active sessions of a Raft group
 */
class RaftSessionRegistry implements IdentifiedDataSerializable {

    private CPGroupId groupId;
    private final Map<Long, CPSessionInfo> sessions = new ConcurrentHashMap<>();
    private long nextSessionId;
    private long generatedThreadId;

    RaftSessionRegistry() {
    }

    RaftSessionRegistry(CPGroupId groupId) {
        this.groupId = groupId;
    }

    CPGroupId groupId() {
        return groupId;
    }

    CPSessionInfo getSession(long sessionId) {
        return sessions.get(sessionId);
    }

    long createNewSession(long sessionTTLMs, Address endpoint, String endpointName, CPSessionOwnerType endpointType,
                          long creationTime) {
        long id = ++nextSessionId;
        long expirationTime = toExpirationTime(creationTime, sessionTTLMs);
        CPSessionInfo session = new CPSessionInfo(id, 0, endpoint, endpointName, endpointType, creationTime, expirationTime);
        sessions.put(id, session);
        return id;
    }

    boolean closeSession(long sessionId) {
        return sessions.remove(sessionId) != null;
    }

    boolean expireSession(long sessionId, long expectedVersion) {
        CPSessionInfo session = sessions.get(sessionId);
        if (session == null) {
            return false;
        }

        if (session.version() != expectedVersion) {
            return false;
        }

        sessions.remove(sessionId);
        return true;
    }

    void heartbeat(long sessionId, long sessionTTLMs) {
        CPSessionInfo session = getSessionOrFail(sessionId);
        sessions.put(sessionId, session.heartbeat(sessionTTLMs));
    }

    void shiftExpirationTimes(long durationMs) {
        for (CPSessionInfo session : sessions.values()) {
            sessions.put(session.id(), session.shiftExpirationTime(durationMs));
        }
    }

    // queried locally
    Collection<BiTuple<Long, Long>> getSessionsToExpire() {
        List<BiTuple<Long, Long>> expired = new ArrayList<>();
        long now = Clock.currentTimeMillis();
        for (CPSessionInfo session : sessions.values()) {
            if (session.isExpired(now)) {
                expired.add(BiTuple.of(session.id(), session.version()));
            }
        }

        return expired;
    }

    private CPSessionInfo getSessionOrFail(long sessionId) {
        CPSessionInfo session = sessions.get(sessionId);
        if (session == null) {
            throw new SessionExpiredException();
        }
        return session;
    }

    Collection<? extends CPSession> getSessions() {
        return sessions.values();
    }

    RaftSessionRegistry cloneForSnapshot() {
        RaftSessionRegistry clone = new RaftSessionRegistry();
        clone.groupId = this.groupId;
        clone.sessions.putAll(this.sessions);
        clone.nextSessionId = this.nextSessionId;
        clone.generatedThreadId = this.generatedThreadId;

        return clone;
    }

    long generateThreadId() {
        return ++generatedThreadId;
    }

    @Override
    public int getFactoryId() {
        return RaftSessionServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftSessionServiceDataSerializerHook.RAFT_SESSION_REGISTRY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(groupId);
        out.writeLong(nextSessionId);
        out.writeInt(sessions.size());
        for (CPSessionInfo session : sessions.values()) {
            out.writeObject(session);
        }
        out.writeLong(generatedThreadId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        nextSessionId = in.readLong();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            CPSessionInfo session = in.readObject();
            sessions.put(session.id(), session);
        }
        generatedThreadId = in.readLong();
    }

}
