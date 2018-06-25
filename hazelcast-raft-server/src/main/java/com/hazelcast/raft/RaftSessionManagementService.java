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

package com.hazelcast.raft;

import com.hazelcast.config.raft.RaftConfig;
import com.hazelcast.core.ICompletableFuture;

import java.util.Collection;

/**
 * The public API used for managing Raft sessions.
 */
public interface RaftSessionManagementService {

    /**
     * Returns a non-null collection of sessions that are currently active in the given Raft group.
     */
    Collection<SessionInfo> getAllSessions(RaftGroupId groupId);

    /**
     * If caller of a Raft session crashes, its session is not automatically terminated immediately.
     * The session will be closed after {@link RaftConfig#sessionTimeToLiveSeconds} sessions.
     * If it is known for sure that the session owner is gone and will not come back,
     * this method can be used for closing the session and releasing its resources immediately.
     */
    ICompletableFuture<Boolean> forceCloseSession(RaftGroupId groupId, long sessionId);

}
