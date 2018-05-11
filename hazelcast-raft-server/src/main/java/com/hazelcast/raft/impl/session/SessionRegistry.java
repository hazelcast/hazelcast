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
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SessionInfo;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.raft.impl.session.Session.toExpirationTime;

/**
 * TODO: Javadoc Pending...
 */
public class SessionRegistry {

    private final RaftGroupId groupId;
    private final Map<Long, Session> sessions = new ConcurrentHashMap<Long, Session>();
    private long nextSessionId;

    SessionRegistry(RaftGroupId groupId) {
        this.groupId = groupId;
    }

    SessionRegistry(RaftGroupId groupId, SessionRegistrySnapshot snapshot) {
        this(groupId);
        this.nextSessionId = snapshot.getNextSessionId();
        for (Session session : snapshot.getSessions()) {
            this.sessions.put(session.id(), session);
        }
    }

    RaftGroupId groupId() {
        return groupId;
    }

    Session getSession(long sessionId) {
        return sessions.get(sessionId);
    }

    long createNewSession(long sessionTTLMs, Address endpoint) {
        long id = nextSessionId++;
        long creationTime = Clock.currentTimeMillis();
        Session session = new Session(id, creationTime, toExpirationTime(creationTime, sessionTTLMs), endpoint);
        sessions.put(id, session);
        return id;
    }

    boolean closeSession(long sessionId) {
        return sessions.remove(sessionId) != null;
    }

    boolean invalidateSession(long sessionId, long expectedVersion) {
        Session session = sessions.get(sessionId);
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
        Session session = getSessionOrFail(sessionId);
        sessions.put(sessionId, session.heartbeat(sessionTTLMs));
    }

    void shiftExpirationTimes(long durationMs) {
        for (Session session : sessions.values()) {
            sessions.put(session.id(), session.shiftExpirationTime(durationMs));
        }
    }

    // queried locally
    Collection<Tuple2<Long, Long>> getExpiredSessions() {
        List<Tuple2<Long, Long>> expired = new ArrayList<Tuple2<Long, Long>>();
        long now = Clock.currentTimeMillis();
        for (Session session : sessions.values()) {
            if (session.isExpired(now)) {
                expired.add(Tuple2.of(session.id(), session.version()));
            }
        }

        return expired;
    }

    SessionRegistrySnapshot toSnapshot() {
        return new SessionRegistrySnapshot(nextSessionId, sessions.values());
    }

    private Session getSessionOrFail(long sessionId) {
        Session session = sessions.get(sessionId);
        if (session == null) {
            throw new SessionExpiredException();
        }
        return session;
    }

    Collection<? extends SessionInfo> getSessions() {
        return sessions.values();
    }
}
