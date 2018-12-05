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

package com.hazelcast.cp.internal.datastructures.spi.blocking;

import com.hazelcast.cp.internal.session.AbstractProxySessionManager;

import java.util.UUID;

/**
 * This abstraction is used when an operation does not return a response
 * at commit-time. Such an operation will either return a response in future
 * or timeout. Future completions of such operations are represented with
 * implementations of this interface.
 */
public interface WaitKey {

    /**
     * Returns id of the session to which the corresponding operation is
     * attached.
     * Returns {@link AbstractProxySessionManager#NO_SESSION_ID} if no session
     *         is attached.
     */
    long sessionId();

    /**
     * Returns commit index of the operation that has not returned a response
     * at the time of its commit.
     */
    long commitIndex();

    /**
     * Returns unique id of the committed operation which is provided by the caller.
     */
    UUID invocationUid();
}
