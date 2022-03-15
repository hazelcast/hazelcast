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

import com.hazelcast.cp.CPGroupId;

/**
 * Used by services to validate and trigger session activity
 */
public interface SessionAccessor {

    /**
     * Checks if there is an active session with the given id in the Raft group
     *
     * @return true if the session is found, false otherwise
     */
    boolean isActive(CPGroupId groupId, long sessionId);

    /**
     * Performs a heartbeat for the given session id in the Raft group.
     *
     * @throws IllegalStateException if there is no {@link RaftSessionRegistry}
     *         initialized for the Raft group
     * @throws SessionExpiredException if there is no active session
     *         with the given id in the Raft group
     */
    void heartbeat(CPGroupId groupId, long sessionId);
}
