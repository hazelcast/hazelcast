/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raft.impl;

import java.util.UUID;

/**
 * Represents an endpoint that runs the Raft consensus algorithm as a member of
 * a Raft group.
 * <p>
 * From the Raft consensus algorithm's point of view, the only important
 * property of a Raft endpoint is its identity. All other properties are
 * implementation details of a Raft endpoint and can be handled inside the
 * {@code RaftIntegration} abstraction.
 */
public interface RaftEndpoint {

    /**
     * Returns the UUID of this Raft endpoint
     *
     * @return the UUID of this Raft endpoint
     */
    UUID getUuid();

}
