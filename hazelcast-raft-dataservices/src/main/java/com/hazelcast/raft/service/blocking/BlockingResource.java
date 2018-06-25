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

package com.hazelcast.raft.service.blocking;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;

/**
 * Operations on a {@link BlockingResource} may not return a response at commit-time.
 * Such operations register {@link WaitKey} instances.
 * Then, their wait keys can be completed in future when some other operations are committed or a timeout occurs.
 *
 * @param <W> concrete type of the WaitKey
 */
public abstract class BlockingResource<W extends WaitKey> implements DataSerializable {

    protected RaftGroupId groupId;
    protected String name;
    protected LinkedList<W> waitKeys = new LinkedList<W>();

    protected BlockingResource() {
    }

    protected BlockingResource(RaftGroupId groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    public final RaftGroupId getGroupId() {
        return groupId;
    }

    public final String getName() {
        return name;
    }

    /**
     * Called when a session is closed.
     * If current state of the resource is attached to the closed session, it must be cleaned up.
     * The second parameter can be filled with new responses which are assigned to some wait keys during the cleanup process.
     */
    protected abstract void onSessionClose(long sessionId, Long2ObjectHashMap<Object> responses);

    /**
     * Returns a non-null collection of session ids that the current state of the resource is attached to.
     * For instance, owner sessions of semaphore permits.
     */
    protected abstract Collection<Long> getActivelyAttachedSessions();

    final List<W> getWaitKeys() {
        return unmodifiableList(waitKeys);
    }

    final boolean expireWaitKey(W key) {
        Iterator<W> iter = waitKeys.iterator();
        while (iter.hasNext()) {
            W k = iter.next();
            if (k.equals(key)) {
                iter.remove();
                return true;
            }
        }

        return false;
    }

    final Map<Long, Object> closeSession(long sessionId) {
        Object expired = new SessionExpiredException();
        Long2ObjectHashMap<Object> result = new Long2ObjectHashMap<Object>();

        Iterator<W> iter = waitKeys.iterator();
        while (iter.hasNext()) {
            W entry = iter.next();
            if (sessionId == entry.sessionId()) {
                result.put(entry.commitIndex(), expired);
                iter.remove();
            }
        }

        onSessionClose(sessionId, result);

        return result;
    }

    final void collectAttachedSessions(Collection<Long> sessions) {
        sessions.addAll(getActivelyAttachedSessions());
        for (WaitKey key : waitKeys) {
            sessions.add(key.sessionId());
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(groupId);
        out.writeUTF(name);
        out.writeInt(waitKeys.size());
        for (W key : waitKeys) {
            out.writeObject(key);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        name = in.readUTF();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            W key = in.readObject();
            waitKeys.add(key);
        }
    }

    @Override
    public String toString() {
        return "BlockingResource{" + "groupId=" + groupId + ", name='" + name + '\'' + ", waitKeys=" + waitKeys + '}';
    }
}
