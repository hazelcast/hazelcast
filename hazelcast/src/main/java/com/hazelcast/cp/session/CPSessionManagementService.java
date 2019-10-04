/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.session;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.ICompletableFuture;

import java.util.Collection;

/**
 * This interface offers API for managing CP sessions.
 * <p>
 * CP sessions are used for tracking liveliness of Hazelcast servers and clients
 * that hold CP resources.
 *
 * @see CPSession
 */
public interface CPSessionManagementService {

    /**
     * Returns a non-null collection of CP sessions that are currently active
     * in the given CP group.
     */
    ICompletableFuture<Collection<CPSession>> getAllSessions(String groupName);

    /**
     * If a Hazelcast instance that owns a CP session crashes, its CP session
     * is not terminated immediately. Instead, the session is closed after
     * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()} passes.
     * If it is known for sure that the session owner is not partitioned away
     * and actually crashed, this method can be used for closing the session and
     * releasing its resources immediately.
     */
    ICompletableFuture<Boolean> forceCloseSession(String groupName, long sessionId);

}
