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

package com.hazelcast.cp.internal.datastructures.spi.blocking;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * Operations on a {@link BlockingResource} may not return a response
 * at commit-time. Such operations register {@link WaitKey} instances.
 * Then, their wait keys can be completed in future when some other operations
 * are committed or a timeout occurs.
 *
 * @param <W> concrete type of the WaitKey
 */
public abstract class BlockingResource<W extends WaitKey> implements DataSerializable {

    // Should be an insertion ordered map to ensure fairness
    protected final Map<Object, WaitKeyContainer<W>> waitKeys = Collections.synchronizedMap(new LinkedHashMap<>());
    private CPGroupId groupId;
    private String name;

    protected BlockingResource() {
    }

    protected BlockingResource(CPGroupId groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    public final CPGroupId getGroupId() {
        return groupId;
    }

    public final String getName() {
        return name;
    }

    // only for testing purposes
    public final Map<Object, WaitKeyContainer<W>> getInternalWaitKeysMap() {
        return waitKeys;
    }

    /**
     * Called when a session is closed.
     * If current state of the resource is attached to the closed session, it must be cleaned up.
     * The second parameter can be filled with new responses which are assigned to some wait keys during the cleanup process.
     */
    protected abstract void onSessionClose(long sessionId, Map<Long, Object> responses);

    /**
     * Returns a non-null collection of session ids that the current state of the resource is attached to.
     * For instance, owner sessions of semaphore permits.
     */
    protected abstract Collection<Long> getActivelyAttachedSessions();

    protected final void addWaitKey(Object waitKeyId, W waitKey) {
        WaitKeyContainer<W> container = waitKeys.get(waitKeyId);
        if (container != null) {
            container.addRetry(waitKey);
        } else {
            waitKeys.put(waitKeyId, new WaitKeyContainer<>(waitKey));
        }
    }

    protected final WaitKeyContainer<W> getWaitKeyContainer(Object waitKeyId) {
        return waitKeys.get(waitKeyId);
    }

    protected final void removeWaitKey(Object waitKeyId) {
        waitKeys.remove(waitKeyId);
    }

    protected final Collection<W> getAllWaitKeys() {
        synchronized (waitKeys) {
            List<W> all = new ArrayList<>(waitKeys.size());
            for (WaitKeyContainer<W> container : waitKeys.values()) {
                all.addAll(container.keyAndRetries());
            }

            return all;
        }
    }

    final void expireWaitKeys(UUID invocationUid, List<W> expired) {
        Iterator<WaitKeyContainer<W>> iter = waitKeys.values().iterator();
        synchronized (waitKeys) {
            while (iter.hasNext()) {
                WaitKeyContainer<W> container = iter.next();
                if (container.invocationUid().equals(invocationUid)) {
                    expired.addAll(container.keyAndRetries());
                    iter.remove();
                    onWaitKeyExpire(container.key());
                    return;
                }
            }
        }
    }

    protected void onWaitKeyExpire(W waitKey) {
    }

    /**
     * To prevent a {@link java.util.ConcurrentModificationException}, all iterator operations
     * should be synchronized on the {@code waitKeys} object
     */
    protected final Iterator<WaitKeyContainer<W>> waitKeyContainersIterator() {
        return waitKeys.values().iterator();
    }

    protected final void clearWaitKeys() {
        waitKeys.clear();
    }

    final void closeSession(long sessionId, List<Long> expiredWaitKeys, Map<Long, Object> result) {
        Iterator<WaitKeyContainer<W>> iter = waitKeys.values().iterator();
        synchronized (waitKeys) {
            while (iter.hasNext()) {
                WaitKeyContainer<W> container = iter.next();
                if (container.sessionId() == sessionId) {
                    for (W retry : container.keyAndRetries()) {
                        expiredWaitKeys.add(retry.commitIndex());
                    }

                    iter.remove();
                }
            }
        }

        onSessionClose(sessionId, result);
    }

    final void collectAttachedSessions(Collection<Long> sessions) {
        sessions.addAll(getActivelyAttachedSessions());
        synchronized (waitKeys) {
            for (WaitKeyContainer<W> key : waitKeys.values()) {
                sessions.add(key.sessionId());
            }
        }
    }

    protected final void cloneForSnapshot(BlockingResource<W> clone) {
        clone.groupId = groupId;
        clone.name = name;
        clone.waitKeys.putAll(waitKeys);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(groupId);
            out.writeString(name);
        synchronized (waitKeys) {
            out.writeInt(waitKeys.size());
            for (Entry<Object, WaitKeyContainer<W>> e : waitKeys.entrySet()) {
                out.writeObject(e.getKey());
                out.writeObject(e.getValue());
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        name = in.readString();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            Object key = in.readObject();
            WaitKeyContainer<W> container = in.readObject();
            waitKeys.put(key, container);
        }
    }

    protected final String internalToString() {
        return "groupId=" + groupId + ", name='" + name + '\'' + ", waitKeys=" + waitKeys;
    }
}
