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

package com.hazelcast.raft.service.blocking;

import com.hazelcast.raft.service.session.AbstractSessionManager;

/**
 * This abstraction is used when an operation does not return a response at commit-time.
 * Such an operation will either return a response in future or timeout.
 * Future completions of such operations are represented with implementations of this interface.
 */
public interface WaitKey {

    /**
     * Returns name of the data structure to which this wait key is attached.
     */
    String name();

    /**
     * Returns id of the session to which the corresponding operation is attached.
     * Returns {@link AbstractSessionManager#NO_SESSION_ID} If no session is attached.
     */
    long sessionId();

    /**
     * Returns commit index of the operation that has not returned a response at the time of its commit.
     */
    long commitIndex();
}
