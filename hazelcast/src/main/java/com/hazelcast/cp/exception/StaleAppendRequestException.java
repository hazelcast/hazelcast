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

package com.hazelcast.cp.exception;

import com.hazelcast.core.IndeterminateOperationState;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;

import java.util.UUID;

/**
 * A {@code CPSubsystemException} which is thrown when a Raft leader node
 * appends an entry to its local Raft log, but demotes to the follower role
 * before learning the commit status of the entry. In this case, this node
 * cannot decide if the operation is committed or not.
 */
public class StaleAppendRequestException extends CPSubsystemException implements IndeterminateOperationState {

    private static final long serialVersionUID = -736303015926722821L;

    public StaleAppendRequestException(RaftEndpoint leader) {
        super(leader != null ? leader.getUuid() : null);
    }

    private StaleAppendRequestException(UUID leaderUuid, Throwable cause) {
        super(null, cause, leaderUuid);
    }

    @Override
    public StaleAppendRequestException wrap() {
        return new StaleAppendRequestException(getLeaderUuid(), this);
    }
}
