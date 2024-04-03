/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.Map;

public interface ProxySessionManager {
    /**
     * Represents absence of a Raft session
     */
    long NO_SESSION_ID = -1;

    long getSession(RaftGroupId groupId);

    // For testing
    long getSessionAcquireCount(RaftGroupId groupId, long sessionId);

    default void shutdownAndAwait() {
    }

    long acquireSession(RaftGroupId groupId);

    /**
     * Increments acquire count of the session.
     * Creates a new session if there is no session yet.
     */
    long acquireSession(RaftGroupId groupId, int count);

    /**
     * Invalidates the given session.
     * No more heartbeats will be sent for the given session.
     */
    void invalidateSession(RaftGroupId groupId, long id);

    void releaseSession(RaftGroupId groupId, long sessionId);

    /**
     * Decrements acquire count of the session.
     * Returns silently if no session exists for the given id.
     */
    void releaseSession(RaftGroupId groupId, long id, int count);

    /**
     * Invokes a shutdown call on server to close all existing sessions.
     */
     Map<RaftGroupId, InternalCompletableFuture<Object>> shutdown();

    Long getOrCreateUniqueThreadId(RaftGroupId groupId);
}
