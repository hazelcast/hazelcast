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

package com.hazelcast.raft.service.session;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.impl.session.SessionResponse;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.hazelcast.util.ExceptionUtil.peel;
import static com.hazelcast.util.Preconditions.checkState;

/**
 * TODO: Javadoc Pending...
 */
public abstract class AbstractSessionManager {

    public static final int NO_SESSION_ID = -1;

    private final ConcurrentMap<RaftGroupId, Object> mutexes = new ConcurrentHashMap<RaftGroupId, Object>();
    private final ConcurrentMap<RaftGroupId, ClientSession> sessions = new ConcurrentHashMap<RaftGroupId, ClientSession>();
    private final AtomicBoolean scheduleHeartbeat = new AtomicBoolean(false);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean running = true;

    public final long acquireSession(RaftGroupId groupId) {
        return getOrCreateSession(groupId).acquire();
    }

    private ClientSession getOrCreateSession(RaftGroupId groupId) {
        lock.readLock().lock();
        try {
            checkState(running, "Session manager is already shut down!");

            ClientSession session = sessions.get(groupId);
            if (session == null || !session.isValid()) {
                synchronized (mutex(groupId)) {
                    session = sessions.get(groupId);
                    if (session == null || !session.isValid()) {
                        session = createNewSession(groupId);
                    }
                }
            }
            return session;
        } finally {
            lock.readLock().unlock();
        }
    }

    private Object mutex(RaftGroupId groupId) {
        Object mutex = mutexes.get(groupId);
        if (mutex != null) {
            return mutex;
        }
        mutex = new Object();
        Object current = mutexes.putIfAbsent(groupId, mutex);
        return current != null ? current : mutex;
    }

    // Creates new session on server
    private ClientSession createNewSession(RaftGroupId groupId) {
        synchronized (mutex(groupId)) {
            SessionResponse response = requestNewSession(groupId);
            ClientSession session = new ClientSession(response.getSessionId(), response.getTtlMillis());
            sessions.put(groupId, session);
            scheduleHeartbeatTask(response.getHeartbeatMillis());
            return session;
        }
    }

    private void scheduleHeartbeatTask(long heartbeatMillis) {
        if (scheduleHeartbeat.compareAndSet(false, true)) {
            scheduleWithRepetition(new HeartbeatTask(), heartbeatMillis, TimeUnit.MILLISECONDS);
        }
    }

    public final void releaseSession(RaftGroupId groupId, long id) {
        ClientSession session = sessions.get(groupId);
        if (session != null && session.id == id) {
            session.release();
        }
    }

    public final void invalidateSession(RaftGroupId groupId, long id) {
        ClientSession session = sessions.get(groupId);
        if (session != null && session.id == id) {
            sessions.remove(groupId, session);
        }
    }

    public final long getSession(RaftGroupId groupId) {
        ClientSession session = sessions.get(groupId);
        return session != null ? session.id : NO_SESSION_ID;
    }

    // For testing
    public final long getSessionUsageCount(RaftGroupId groupId, long sessionId) {
        ClientSession session = sessions.get(groupId);
        return session != null && session.id == sessionId ? session.operationsCount.get() : 0;
    }

    public final Map<RaftGroupId, ICompletableFuture<Object>> shutdown() {
        lock.writeLock().lock();
        try {
            Map<RaftGroupId, ICompletableFuture<Object>> futures = new HashMap<RaftGroupId, ICompletableFuture<Object>>();
            for (Entry<RaftGroupId, ClientSession> e : sessions.entrySet()) {
                RaftGroupId groupId = e.getKey();
                long sessionId = e.getValue().id;
                ICompletableFuture<Object> f = closeSession(groupId, sessionId);
                futures.put(groupId, f);
            }
            sessions.clear();
            running = false;
            return futures;
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected abstract SessionResponse requestNewSession(RaftGroupId groupId);

    protected abstract ScheduledFuture<?> scheduleWithRepetition(Runnable task, long period, TimeUnit unit);

    protected abstract ICompletableFuture<Object> heartbeat(RaftGroupId groupId, long sessionId);

    protected abstract ICompletableFuture<Object> closeSession(RaftGroupId groupId, Long sessionId);


    private static class ClientSession {
        private final long id;
        private final AtomicInteger operationsCount = new AtomicInteger();

        private final long ttlMillis;
        private volatile long accessTime;

        ClientSession(long id, long ttlMillis) {
            this.id = id;
            this.accessTime = Clock.currentTimeMillis();
            this.ttlMillis = ttlMillis;
        }

        boolean isValid() {
            return isInUse() || !isExpired(Clock.currentTimeMillis());
        }

        boolean isInUse() {
            return operationsCount.get() > 0;
        }

        private boolean isExpired(long timestamp) {
            long expirationTime = accessTime + ttlMillis;
            if (expirationTime < 0) {
                expirationTime = Long.MAX_VALUE;
            }
            return timestamp > expirationTime;
        }

        long acquire() {
            operationsCount.incrementAndGet();
            return id;
        }

        void release() {
            operationsCount.decrementAndGet();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ClientSession)) {
                return false;
            }

            ClientSession that = (ClientSession) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private class HeartbeatTask implements Runnable {
        // HeartbeatTask executions will not overlap.
        private final Collection<ICompletableFuture<Object>> prevHeartbeats = new ArrayList<ICompletableFuture<Object>>();

        @Override
        public void run() {
            for (ICompletableFuture<Object> future : prevHeartbeats) {
                future.cancel(true);
            }
            prevHeartbeats.clear();

            for (Entry<RaftGroupId, ClientSession> entry : sessions.entrySet()) {
                final RaftGroupId groupId = entry.getKey();
                final ClientSession session = entry.getValue();
                if (session.isInUse()) {
                    ICompletableFuture<Object> f = heartbeat(groupId, session.id);
                    f.andThen(new ExecutionCallback<Object>() {
                        @Override
                        public void onResponse(Object response) {
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            if (peel(t) instanceof SessionExpiredException) {
                                invalidateSession(groupId, session.id);
                            }
                        }
                    });
                    prevHeartbeats.add(f);
                }
            }
        }
    }
}
