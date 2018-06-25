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

package com.hazelcast.raft.service.semaphore;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.BlockingResource;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.Collections.unmodifiableCollection;

/**
 * State-machine implementation of the Raft-based semaphore.
 * Supports both sessionless and session-aware semaphore proxies.
 */
public class RaftSemaphore extends BlockingResource<SemaphoreInvocationKey> implements IdentifiedDataSerializable {

    private boolean initialized;
    private int available;
    private final Map<Long, SessionSemaphoreState> sessionStates = new HashMap<Long, SessionSemaphoreState>();

    RaftSemaphore() {
    }

    RaftSemaphore(RaftGroupId groupId, String name) {
        super(groupId, name);
    }

    Collection<SemaphoreInvocationKey> init(int permits) {
        if (initialized || available != 0) {
            throw new IllegalStateException();
        }

        available = permits;
        initialized = true;

        return assignPermitsToWaitKeys();
    }

    int getAvailable() {
        return available;
    }

    boolean isAvailable(int permits) {
        checkPositive(permits, "Permits should be positive!");
        return available >= permits;
    }

    /**
     * Assigns permits to the endpoint, if sufficient number of permits are available.
     * If there are no sufficient number of permits and the second argument is true,
     * a wait key is created and added to the wait queue.
     * Permits are not assigned if the acquire request is a retry of a successful acquire request of a session-aware proxy.
     * Permits are assigned again if the acquire request is a retry of a successful acquire request of a sessionless proxy.
     * If the acquire request is a retry of an endpoint that resides in the wait queue with the same invocation uid,
     * a duplicate wait key is added to the wait queue because cancelling the previous wait key can cause the caller to fail.
     * If the acquire request is a new request of an endpoint that resides in the wait queue with a different invocation uid,
     * the existing wait key is cancelled because it means the caller has stopped waiting for response of the previous invocation.
     */
    AcquireResult acquire(SemaphoreInvocationKey key, boolean wait) {
        SessionSemaphoreState state = sessionStates.get(key.sessionId());
        if (state != null && state.invocationRefUids.containsKey(Tuple2.of(key.threadId(), key.invocationUid()))) {
            return new AcquireResult(key.permits(), Collections.<SemaphoreInvocationKey>emptyList());
        }

        Collection<SemaphoreInvocationKey> cancelled = cancelWaitKeys(key.sessionId(), key.threadId(), key.invocationUid());

        if (!isAvailable(key.permits())) {
            if (wait) {
                waitKeys.add(key);
            }

            return new AcquireResult(0, cancelled);
        }

        assignPermitsToInvocation(key.sessionId(), key.threadId(), key.invocationUid(), key.permits());

        return new AcquireResult(key.permits(), cancelled);
    }

    private void assignPermitsToInvocation(long sessionId, long threadId, UUID invocationUid, int permits) {
        if (sessionId == NO_SESSION_ID) {
            available -= permits;
            return;
        }

        SessionSemaphoreState state = sessionStates.get(sessionId);
        if (state == null) {
            state = new SessionSemaphoreState();
            sessionStates.put(sessionId, state);
        }

        if (state.invocationRefUids.put(Tuple2.of(threadId, invocationUid), permits) == null) {
            state.acquiredPermits += permits;
            available -= permits;
        }
    }

    /**
     * Releases the given number of permits.
     * Permits are not released if it is a retry of a previous successful release request of a session-aware proxy.
     * Permits are released again if it is a retry of a successful release request of a sessionless proxy.
     * If the release request fails because the requesting endpoint does not hold the given number of permits, all wait keys
     * of the endpoint are cancelled because that endpoint has stopped waiting for response of the previous acquire() invocation.
     * Returns completed wait keys after successful release if there are any.
     * Returns cancelled wait keys after failed release if there are any.
     */
    ReleaseResult release(long sessionId, long threadId, UUID invocationUid, int permits) {
        checkPositive(permits, "Permits should be positive!");

        if (sessionId != NO_SESSION_ID) {
            SessionSemaphoreState state = sessionStates.get(sessionId);
            if (state == null) {
                return ReleaseResult.failed(cancelWaitKeys(sessionId, threadId, invocationUid));
            }

            if (state.invocationRefUids.containsKey(Tuple2.of(threadId, invocationUid))) {
                return ReleaseResult.successful(Collections.<SemaphoreInvocationKey>emptyList(),
                        Collections.<SemaphoreInvocationKey>emptyList());
            }

            if (state.acquiredPermits < permits) {
                return ReleaseResult.failed(cancelWaitKeys(sessionId, threadId, invocationUid));
            }

            state.acquiredPermits -= permits;
            state.invocationRefUids.put(Tuple2.of(threadId, invocationUid), permits);
        }

        available += permits;

        // order is important...
        Collection<SemaphoreInvocationKey> cancelled = cancelWaitKeys(sessionId, threadId, invocationUid);
        Collection<SemaphoreInvocationKey> acquired = assignPermitsToWaitKeys();

        return ReleaseResult.successful(acquired, cancelled);
    }

    private Collection<SemaphoreInvocationKey> cancelWaitKeys(long sessionId, long threadId, UUID invocationUid) {
        List<SemaphoreInvocationKey> cancelled = new ArrayList<SemaphoreInvocationKey>(0);
        Iterator<SemaphoreInvocationKey> iter = waitKeys.iterator();
        while (iter.hasNext()) {
            SemaphoreInvocationKey waitKey = iter.next();
            if (waitKey.sessionId() == sessionId && waitKey.threadId() == threadId
                    && !waitKey.invocationUid().equals(invocationUid)) {
                cancelled.add(waitKey);
                iter.remove();
            }
        }

        return cancelled;
    }

    private Collection<SemaphoreInvocationKey> assignPermitsToWaitKeys() {
        List<SemaphoreInvocationKey> assigned = new ArrayList<SemaphoreInvocationKey>();
        Set<UUID> assignedInvocationUids = new HashSet<UUID>();
        Iterator<SemaphoreInvocationKey> iterator = waitKeys.iterator();
        while (iterator.hasNext()) {
            SemaphoreInvocationKey key = iterator.next();
            if (assignedInvocationUids.contains(key.invocationUid())) {
                iterator.remove();
                assigned.add(key);
            } else if (key.permits() <= available) {
                iterator.remove();
                if (assignedInvocationUids.add(key.invocationUid())) {
                    assigned.add(key);
                    assignPermitsToInvocation(key.sessionId(), key.threadId(), key.invocationUid(), key.permits());
                }
            }
        }

        return assigned;
    }

    /**
     * Assigns all available permits to the <sessionId, threadId> endpoint.
     * Permits are not assigned if the drain request is a retry of a successful drain request of a session-aware proxy.
     * Permits are assigned again if the drain request is a retry of a successful drain request of a sessionless proxy.
     * Returns cancelled wait keys of the same endpoint if there are any.
     */
    AcquireResult drain(long sessionId, long threadId, UUID invocationUid) {
        SessionSemaphoreState state = sessionStates.get(sessionId);
        if (state != null) {
            Integer permits = state.invocationRefUids.get(Tuple2.of(threadId, invocationUid));
            if (permits != null) {
                return new AcquireResult(permits, Collections.<SemaphoreInvocationKey>emptyList());
            }
        }

        Collection<SemaphoreInvocationKey> cancelled = cancelWaitKeys(sessionId, threadId, invocationUid);

        int drained = available;
        if (drained > 0) {
            assignPermitsToInvocation(sessionId, threadId, invocationUid, drained);
        }
        available = 0;

        return new AcquireResult(drained, cancelled);
    }

    /**
     * Changes the number of permits by adding the given permit value.
     * Permits are not changed if it is a retry of a previous successful change request of a session-aware proxy.
     * Permits are changed again if it is a retry of a successful change request of a sessionless proxy.
     * If number of permits increase, new assignments can be done.
     * Returns completed wait keys after successful change if there are any.
     * Returns cancelled wait keys of the same endpoint if there are any.
     */
    ReleaseResult change(long sessionId, long threadId, UUID invocationUid, int permits) {
        if (permits == 0) {
            return ReleaseResult.failed(Collections.<SemaphoreInvocationKey>emptyList());
        }

        Collection<SemaphoreInvocationKey> cancelled = cancelWaitKeys(sessionId, threadId, invocationUid);

        if (sessionId != NO_SESSION_ID) {
            SessionSemaphoreState state = sessionStates.get(sessionId);
            if (state == null) {
                state = new SessionSemaphoreState();
                sessionStates.put(sessionId, state);
            }

            if (state.invocationRefUids.containsKey(Tuple2.of(threadId, invocationUid))) {
                Collection<SemaphoreInvocationKey> c = Collections.emptyList();
                return ReleaseResult.successful(c, c);
            }

            state.invocationRefUids.put(Tuple2.of(threadId, invocationUid), permits);
        }

        available += permits;
        initialized = true;

        Collection<SemaphoreInvocationKey> acquired =
                permits > 0 ? assignPermitsToWaitKeys() : Collections.<SemaphoreInvocationKey>emptyList();

        return ReleaseResult.successful(acquired, cancelled);
    }

    /**
     * Releases permits of the closed session.
     */
    @Override
    protected void onSessionClose(long sessionId, Long2ObjectHashMap<Object> responses) {
        SessionSemaphoreState state = sessionStates.get(sessionId);
        if (state != null && state.acquiredPermits > 0) {
            // remove the session after release() because release() checks existence of the session
            ReleaseResult result = release(sessionId, 0, newUnsecureUUID(), state.acquiredPermits);
            sessionStates.remove(sessionId);
            assert result.cancelled.isEmpty();
            for (SemaphoreInvocationKey key : result.acquired) {
                responses.put(key.commitIndex(), Boolean.TRUE);
            }
        }
    }

    @Override
    protected Collection<Long> getActivelyAttachedSessions() {
        Set<Long> activeSessionIds = new HashSet<Long>();
        for (Entry<Long, SessionSemaphoreState> e : sessionStates.entrySet()) {
            if (e.getValue().acquiredPermits > 0) {
                activeSessionIds.add(e.getKey());
            }
        }

        return activeSessionIds;
    }

    @Override
    public int getFactoryId() {
        return RaftSemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.RAFT_SEMAPHORE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeBoolean(initialized);
        out.writeInt(available);
        out.writeInt(sessionStates.size());
        for (Entry<Long, SessionSemaphoreState> e1 : sessionStates.entrySet()) {
            out.writeLong(e1.getKey());
            SessionSemaphoreState state = e1.getValue();
            out.writeInt(state.invocationRefUids.size());
            for (Entry<Tuple2<Long, UUID>, Integer> e2 : state.invocationRefUids.entrySet()) {
                Tuple2<Long, UUID> t = e2.getKey();
                UUID invocationUid = t.element2;
                int permits = e2.getValue();
                out.writeLong(t.element1);
                out.writeLong(invocationUid.getLeastSignificantBits());
                out.writeLong(invocationUid.getMostSignificantBits());
                out.writeInt(permits);
            }
            out.writeInt(state.acquiredPermits);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        initialized = in.readBoolean();
        available = in.readInt();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            long sessionId = in.readLong();
            SessionSemaphoreState state = new SessionSemaphoreState();
            int refUidCount = in.readInt();
            for (int j = 0; j < refUidCount; j++) {
                long threadId = in.readLong();
                long least = in.readLong();
                long most = in.readLong();
                int permits = in.readInt();
                state.invocationRefUids.put(Tuple2.of(threadId, new UUID(most, least)), permits);
            }

            state.acquiredPermits = in.readInt();

            sessionStates.put(sessionId, state);
        }
    }

    @Override
    public String toString() {
        return "RaftSemaphore{" + "groupId=" + groupId + ", name='" + name + '\'' + ", initialized=" + initialized
                + ", available=" + available + ", sessionStates=" + sessionStates + ", waitKeys=" + waitKeys + '}';
    }

    /**
     * Represents result of an acquire() request
     */
    static final class AcquireResult {

        /**
         * Number of acquired permits
         */
        final int acquired;

        /**
         * Cancelled wait keys of the caller if there is any, independent of the acquire request is successful or not.
         */
        final Collection<SemaphoreInvocationKey> cancelled;

        private AcquireResult(int acquired, Collection<SemaphoreInvocationKey> cancelled) {
            this.acquired = acquired;
            this.cancelled = unmodifiableCollection(cancelled);
        }
    }

    /**
     * Represents result of a release() request
     */
    static final class ReleaseResult {

        /**
         * true if the release() request is successful
         */
        final boolean success;

        /**
         * If the release() request is successful and permits are assigned to some other endpoints, contains their wait keys.
         */
        final Collection<SemaphoreInvocationKey> acquired;

        /**
         * Cancelled wait keys of the caller if there is any, independent of the release request is successful or not.
         */
        final Collection<SemaphoreInvocationKey> cancelled;

        private ReleaseResult(boolean success, Collection<SemaphoreInvocationKey> acquired,
                              Collection<SemaphoreInvocationKey> cancelled) {
            this.success = success;
            this.acquired = unmodifiableCollection(acquired);
            this.cancelled = unmodifiableCollection(cancelled);
        }

        private static ReleaseResult successful(Collection<SemaphoreInvocationKey> acquired,
                                                Collection<SemaphoreInvocationKey> cancelled) {
            return new ReleaseResult(true, acquired, cancelled);
        }

        private static ReleaseResult failed(Collection<SemaphoreInvocationKey> cancelled) {
            return new ReleaseResult(false, Collections.<SemaphoreInvocationKey>emptyList(), cancelled);
        }
    }

    private static class SessionSemaphoreState {

        /**
         * map of <threadId, invocationUid> -> permits to track last operation of each endpoint
         */
        private final Map<Tuple2<Long, UUID>, Integer> invocationRefUids = new HashMap<Tuple2<Long, UUID>, Integer>();

        private int acquiredPermits;

        @Override
        public String toString() {
            return "SessionState{" + "invocationRefUids=" + invocationRefUids + ", acquiredPermits=" + acquiredPermits + '}';
        }
    }

}
