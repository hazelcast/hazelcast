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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus;
import com.hazelcast.cp.internal.datastructures.spi.blocking.BlockingResource;
import com.hazelcast.cp.internal.datastructures.spi.blocking.WaitKeyContainer;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.internal.util.collection.Long2ObjectHashMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.FAILED;
import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.SUCCESSFUL;
import static com.hazelcast.cp.internal.datastructures.semaphore.AcquireResult.AcquireStatus.WAIT_KEY_ADDED;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;

/**
 * State-machine implementation of the Raft-based semaphore.
 * Supports both sessionless and session-aware semaphore proxies.
 */
public class Semaphore extends BlockingResource<AcquireInvocationKey> implements IdentifiedDataSerializable {

    private volatile boolean initialized;
    private volatile int available;
    private final Long2ObjectHashMap<SessionSemaphoreState> sessionStates = new Long2ObjectHashMap<>();

    Semaphore() {
    }

    Semaphore(CPGroupId groupId, String name, int available) {
        super(groupId, name);
        checkNotNegative(available, "Initial permit count cannot be negative: " + available + " for Semaphore: "
                + name);
        this.available = available;
        this.initialized = available > 0;
    }

    Collection<AcquireInvocationKey> init(int permits) {
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

    boolean isInitialized() {
        return initialized;
    }

    /**
     * Assigns permits to the endpoint, if sufficient number of permits are
     * available. If there are no sufficient number of permits and the second
     * argument is true, a wait key is created and added to the wait queue.
     * Permits are not assigned if the acquire request is a retry of
     * a successful acquire request of a session-aware proxy. Permits are
     * assigned again if the acquire request is a retry of a successful acquire
     * request of a sessionless proxy. If the acquire request is a retry of
     * an endpoint that resides in the wait queue with the same invocation uid,
     * a duplicate wait key is added to the wait queue because cancelling
     * the previous wait key can cause the caller to fail. If the acquire
     * request is a new request of an endpoint that resides in the wait queue
     * with a different invocation uid, the existing wait key is cancelled
     * because it means the caller has stopped waiting for response of
     * the previous invocation.
     */
    AcquireResult acquire(AcquireInvocationKey key, boolean wait) {
        SemaphoreEndpoint endpoint = key.endpoint();
        SessionSemaphoreState state = sessionStates.get(key.sessionId());
        if (state != null) {
            Integer acquired = state.getInvocationResponse(endpoint.threadId(), key.invocationUid());
            if (acquired != null) {
                AcquireStatus status = acquired > 0 ? SUCCESSFUL : FAILED;
                return new AcquireResult(status, acquired, Collections.emptyList());
            }
        }

        Collection<AcquireInvocationKey> cancelled = cancelWaitKeys(endpoint, key.invocationUid());

        if (!isAvailable(key.permits())) {
            AcquireStatus status;
            if (wait) {
                addWaitKey(endpoint, key);
                status = WAIT_KEY_ADDED;
            } else {
                assignPermitsToInvocation(endpoint, key.invocationUid(), 0);
                status = FAILED;
            }

            return new AcquireResult(status, 0, cancelled);
        }

        assignPermitsToInvocation(endpoint, key.invocationUid(), key.permits());

        return new AcquireResult(SUCCESSFUL, key.permits(), cancelled);
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void assignPermitsToInvocation(SemaphoreEndpoint endpoint, UUID invocationUid, int permits) {
        long sessionId = endpoint.sessionId();
        if (sessionId == NO_SESSION_ID) {
            available -= permits;
            return;
        }

        SessionSemaphoreState state = sessionStates.get(sessionId);
        if (state == null) {
            state = new SessionSemaphoreState();
            sessionStates.put(sessionId, state);
        }

        BiTuple<UUID, Integer> prev = state.invocationRefUids.put(endpoint.threadId(), BiTuple.of(invocationUid, permits));
        if (prev == null || !prev.element1.equals(invocationUid)) {
            state.acquiredPermits += permits;
            available -= permits;
        }
    }

    /**
     * Releases the given number of permits.
     * Permits are not released if it is a retry of a previous successful
     * release request of a session-aware proxy. Permits are released again if
     * it is a retry of a successful release request of a sessionless proxy.
     * If the release request fails because the requesting endpoint does not
     * hold the given number of permits, all wait keys of the endpoint are
     * cancelled because that endpoint has stopped waiting for response of
     * the previous acquire() invocation. Returns completed wait keys after
     * successful release if there are any. Returns cancelled wait keys after
     * failed release if there are any.
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    ReleaseResult release(SemaphoreEndpoint endpoint, UUID invocationUid, int permits) {
        checkPositive(permits, "Permits should be positive!");

        long sessionId = endpoint.sessionId();
        if (sessionId != NO_SESSION_ID) {
            SessionSemaphoreState state = sessionStates.get(sessionId);
            if (state == null) {
                return ReleaseResult.failed(cancelWaitKeys(endpoint, invocationUid));
            }

            Integer response = state.getInvocationResponse(endpoint.threadId(), invocationUid);
            if (response != null) {
                if (response > 0) {
                    return ReleaseResult.successful(Collections.emptyList(), Collections.emptyList());
                } else {
                    return ReleaseResult.failed(cancelWaitKeys(endpoint, invocationUid));
                }
            }

            if (state.acquiredPermits < permits) {
                state.invocationRefUids.put(endpoint.threadId(), BiTuple.of(invocationUid, 0));
                return ReleaseResult.failed(cancelWaitKeys(endpoint, invocationUid));
            }

            state.acquiredPermits -= permits;
            state.invocationRefUids.put(endpoint.threadId(), BiTuple.of(invocationUid, permits));
        }

        available += permits;

        // order is important...
        Collection<AcquireInvocationKey> cancelled = cancelWaitKeys(endpoint, invocationUid);
        Collection<AcquireInvocationKey> acquired = assignPermitsToWaitKeys();

        return ReleaseResult.successful(acquired, cancelled);
    }

    Semaphore cloneForSnapshot() {
        Semaphore clone = new Semaphore();
        cloneForSnapshot(clone);
        clone.initialized = this.initialized;
        clone.available = this.available;
        for (Entry<Long, SessionSemaphoreState> e : this.sessionStates.entrySet()) {
            SessionSemaphoreState s = new SessionSemaphoreState();
            s.acquiredPermits = e.getValue().acquiredPermits;
            s.invocationRefUids.putAll(e.getValue().invocationRefUids);
            clone.sessionStates.put(e.getKey(), s);
        }

        return clone;
    }

    private Collection<AcquireInvocationKey> cancelWaitKeys(SemaphoreEndpoint endpoint, UUID invocationUid) {
        Collection<AcquireInvocationKey> cancelled = null;
        WaitKeyContainer<AcquireInvocationKey> container = getWaitKeyContainer(endpoint);
        if (container != null && container.key().isDifferentInvocationOf(endpoint, invocationUid)) {
            cancelled = container.keyAndRetries();
            removeWaitKey(endpoint);
        }

        return cancelled != null ? cancelled : Collections.emptyList();
    }

    private Collection<AcquireInvocationKey> assignPermitsToWaitKeys() {
        List<AcquireInvocationKey> assigned = new ArrayList<>();
        Iterator<WaitKeyContainer<AcquireInvocationKey>> iterator = waitKeyContainersIterator();
        synchronized (waitKeys) {
            while (iterator.hasNext() && available > 0) {
                WaitKeyContainer<AcquireInvocationKey> container = iterator.next();
                AcquireInvocationKey key = container.key();
                if (key.permits() <= available) {
                    iterator.remove();
                    assigned.addAll(container.keyAndRetries());
                    assignPermitsToInvocation(key.endpoint(), key.invocationUid(), key.permits());
                }
            }

            return assigned;
        }
    }

    /**
     * Assigns all available permits to the <sessionId, threadId> endpoint.
     * Permits are not assigned if the drain request is a retry of a successful
     * drain request of a session-aware proxy. Permits are assigned again if
     * the drain request is a retry of a successful drain request of
     * a sessionless proxy. Returns cancelled wait keys of the same endpoint
     * if there are any.
     */
    AcquireResult drain(SemaphoreEndpoint endpoint, UUID invocationUid) {
        SessionSemaphoreState state = sessionStates.get(endpoint.sessionId());
        if (state != null) {
            Integer permits = state.getInvocationResponse(endpoint.threadId(), invocationUid);
            if (permits != null) {
                return new AcquireResult(SUCCESSFUL, permits, Collections.emptyList());
            }
        }

        Collection<AcquireInvocationKey> cancelled = cancelWaitKeys(endpoint, invocationUid);

        int drained = available;
        assignPermitsToInvocation(endpoint, invocationUid, drained);
        available = 0;

        return new AcquireResult(SUCCESSFUL, drained, cancelled);
    }

    /**
     * Changes the number of permits by adding the given permit value. Permits
     * are not changed if it is a retry of a previous successful change request
     * of a session-aware proxy. Permits are changed again if it is a retry of
     * a successful change request of a sessionless proxy. If number of permits
     * increase, new assignments can be done. Returns completed wait keys after
     * successful change if there are any. Returns cancelled wait keys of
     * the same endpoint if there are any.
     */
    ReleaseResult change(SemaphoreEndpoint endpoint, UUID invocationUid, int permits) {
        if (permits == 0) {
            return ReleaseResult.failed(Collections.emptyList());
        }

        Collection<AcquireInvocationKey> cancelled = cancelWaitKeys(endpoint, invocationUid);

        long sessionId = endpoint.sessionId();
        if (sessionId != NO_SESSION_ID) {
            SessionSemaphoreState state = sessionStates.get(sessionId);
            if (state == null) {
                state = new SessionSemaphoreState();
                sessionStates.put(sessionId, state);
            }

            long threadId = endpoint.threadId();
            Integer response = state.getInvocationResponse(threadId, invocationUid);
            if (response != null) {
                Collection<AcquireInvocationKey> c = Collections.emptyList();
                return ReleaseResult.successful(c, c);
            }

            state.invocationRefUids.put(threadId, BiTuple.of(invocationUid, permits));
        }

        available += permits;
        initialized = true;

        Collection<AcquireInvocationKey> acquired = permits > 0 ? assignPermitsToWaitKeys() : Collections.emptyList();
        return ReleaseResult.successful(acquired, cancelled);
    }

    /**
     * Releases permits of the closed session.
     */
    @Override
    protected void onSessionClose(long sessionId, Map<Long, Object> responses) {
        SessionSemaphoreState state = sessionStates.get(sessionId);
        if (state != null) {
            // remove the session after release() because release() checks existence of the session
            if (state.acquiredPermits > 0) {
                SemaphoreEndpoint endpoint = new SemaphoreEndpoint(sessionId, 0);
                ReleaseResult result = release(endpoint, newUnsecureUUID(), state.acquiredPermits);
                assert result.cancelledWaitKeys().isEmpty();
                for (AcquireInvocationKey key : result.acquiredWaitKeys()) {
                    responses.put(key.commitIndex(), Boolean.TRUE);
                }
            }

            sessionStates.remove(sessionId);
        }
    }

    @Override
    protected Collection<Long> getActivelyAttachedSessions() {
        Set<Long> activeSessionIds = new HashSet<>();
        for (Entry<Long, SessionSemaphoreState> e : sessionStates.entrySet()) {
            if (e.getValue().acquiredPermits > 0) {
                activeSessionIds.add(e.getKey());
            }
        }

        return activeSessionIds;
    }

    @Override
    protected void onWaitKeyExpire(AcquireInvocationKey key) {
        assignPermitsToInvocation(key.endpoint(), key.invocationUid(), 0);
    }

    @Override
    public int getFactoryId() {
        return SemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SemaphoreDataSerializerHook.RAFT_SEMAPHORE;
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
            for (Entry<Long, BiTuple<UUID, Integer>> e2 : state.invocationRefUids.entrySet()) {
                out.writeLong(e2.getKey());
                BiTuple<UUID, Integer> t = e2.getValue();
                writeUUID(out, t.element1);
                out.writeInt(t.element2);
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
                UUID invocationUid = readUUID(in);
                int permits = in.readInt();
                state.invocationRefUids.put(threadId, BiTuple.of(invocationUid, permits));
            }

            state.acquiredPermits = in.readInt();

            sessionStates.put(sessionId, state);
        }
    }

    @Override
    public String toString() {
        return "Semaphore{" + internalToString() + ", initialized=" + initialized
                + ", available=" + available + ", sessionStates=" + sessionStates + '}';
    }

    private static class SessionSemaphoreState {

        /**
         * map of threadId -> <invocationUid, permits> to track last operation of each endpoint
         */
        private final Long2ObjectHashMap<BiTuple<UUID, Integer>> invocationRefUids = new Long2ObjectHashMap<>();

        private int acquiredPermits;

        Integer getInvocationResponse(long threadId, UUID invocationUid) {
            BiTuple<UUID, Integer> t = invocationRefUids.get(threadId);
            return (t != null && t.element1.equals(invocationUid)) ? t.element2 : null;
        }

        @Override
        public String toString() {
            return "SessionState{" + "invocationRefUids=" + invocationRefUids + ", acquiredPermits=" + acquiredPermits + '}';
        }
    }

}
