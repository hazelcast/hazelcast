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

import java.util.Collection;

/**
 * Services that attach their resources to Raft sessions implement this service
 */
public interface SessionAwareService {

    /**
     * Injects {@link SessionAccessor} in order to enable the custom service to
     * validate and trigger session activity
     */
    void setSessionAccessor(SessionAccessor accessor);

    /**
     * Called when a session is closed.
     */
    void onSessionClose(CPGroupId groupId, long sessionId);

    /**
     * Returns a collection of session ids that this service has attached some
     * resources.
     */
    Collection<Long> getAttachedSessions(CPGroupId groupId);

}
