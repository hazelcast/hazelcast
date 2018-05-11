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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * TODO: Javadoc Pending...
 */
public class SessionRegistrySnapshot implements IdentifiedDataSerializable {

    private long nextSessionId;
    private Set<Session> sessions;

    public SessionRegistrySnapshot() {
    }

    SessionRegistrySnapshot(long nextSessionId, Collection<Session> sessions) {
        this.nextSessionId = nextSessionId;
        this.sessions = new HashSet<Session>(sessions);
    }

    long getNextSessionId() {
        return nextSessionId;
    }

    Collection<Session> getSessions() {
        return sessions;
    }

    @Override
    public int getFactoryId() {
        return RaftSessionServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSessionServiceDataSerializerHook.SESSION_REGISTRY_SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(nextSessionId);
        out.writeInt(sessions.size());
        for (Session session : sessions) {
            out.writeLong(session.id());
            out.writeLong(session.creationTime());
            out.writeLong(session.expirationTime());
            out.writeLong(session.version());
            out.writeObject(session.endpoint());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        nextSessionId = in.readLong();
        int size = in.readInt();
        sessions = new HashSet<Session>(size);
        for (int i = 0; i < size; i++) {
            long id = in.readLong();
            long creationTime = in.readLong();
            long expirationTime = in.readLong();
            long version = in.readLong();
            Address endpoint = in.readObject();
            sessions.add(new Session(id, creationTime, expirationTime, version, endpoint));
        }
    }

    @Override
    public String toString() {
        return "SessionRegistrySnapshot{" + "nextSessionId=" + nextSessionId + ", sessions=" + sessions + '}';
    }
}
