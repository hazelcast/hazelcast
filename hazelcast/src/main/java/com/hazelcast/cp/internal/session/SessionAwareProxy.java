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

import com.hazelcast.cp.internal.RaftGroupId;

/**
 * Base class for server and client proxies that make use of Raft sessions
 */
public abstract class SessionAwareProxy {

    protected final RaftGroupId groupId;
    private final AbstractProxySessionManager sessionManager;

    protected SessionAwareProxy(AbstractProxySessionManager sessionManager, RaftGroupId groupId) {
        this.sessionManager = sessionManager;
        this.groupId = groupId;
    }

    public final RaftGroupId getGroupId() {
        return groupId;
    }

    protected final Long getOrCreateUniqueThreadId() {
        return sessionManager.getOrCreateUniqueThreadId(groupId);
    }

    /**
     * Increments acquire count of the session.
     * Creates a new session if there is no session yet.
     */
    protected final long acquireSession() {
        return sessionManager.acquireSession(groupId);
    }

    /**
     * Increments acquire count of the session.
     * Creates a new session if there is no session yet.
     */
    protected final long acquireSession(int count) {
        return sessionManager.acquireSession(groupId, count);
    }

    /**
     * Decrements acquire count of the session.
     * Returns silently if no session exists for the given id.
     */
    protected final void releaseSession(long sessionId) {
        sessionManager.releaseSession(groupId, sessionId);
    }

    /**
     * Decrements acquire count of the session.
     * Returns silently if no session exists for the given id.
     */
    protected final void releaseSession(long sessionId, int count) {
        sessionManager.releaseSession(groupId, sessionId, count);
    }

    /**
     * Invalidates the given session.
     * No more heartbeats will be sent for the given session.
     */
    protected final void invalidateSession(long sessionId) {
        sessionManager.invalidateSession(groupId, sessionId);
    }

    /**
     * Returns id of the session opened for the given Raft group.
     * Returns {@link AbstractProxySessionManager#NO_SESSION_ID}
     *         if no session exists.
     */
    protected final long getSession() {
        return sessionManager.getSession(groupId);
    }
}
