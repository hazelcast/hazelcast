/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

/**
 * Possible states of a {@link Node} during its lifecycle.
 * Some actions/operations may be allowed or denied
 * according to current state of the {@link Node}.
 *
 * @see Node#start()
 * @see Node#shutdown(boolean)
 * @see com.hazelcast.spi.impl.AllowedDuringShutdown
 *
 * @since 3.6
 */
public enum NodeState {

    /**
     * Initial state of the Node. An {@code ACTIVE} node is allowed
     * to execute/process all kinds of operations.
     */
    ACTIVE,

    /**
     * When {@link Node#shutdown(boolean)} is called, node will go into {@code SHUTTING_DOWN}
     * until shutdown process completes.
     * <p/>
     * In {@code SHUTTING_DOWN} state, all operations will be rejected except replication/migration
     * operations and heartbeat operations. Operations those are to be allowed during {@code SHUTTING_DOWN}
     * state should be marked as {@link com.hazelcast.spi.impl.AllowedDuringShutdown}.
     * <p/>
     * Once shutdown completes, state will become {@link #SHUT_DOWN}.
     */
    SHUTTING_DOWN,

    /**
     * After {@link Node#shutdown(boolean)} call ends, node's state will be {@code SHUT_DOWN}. In {@code SHUT_DOWN}
     * state node will be completely inactive. All operations/invocations will be rejected. Once a node is shutdown,
     * it cannot be restarted.
     */
    SHUT_DOWN
}
