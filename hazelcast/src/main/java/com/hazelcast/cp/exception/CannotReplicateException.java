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

import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.spi.exception.RetryableException;

import java.util.UUID;

/**
 * A {@code CPSubsystemException} which is thrown when an entry cannot be
 * replicated, which can occur in one of the following cases:
 * <ul>
 * <li>a member leaves the CP group</li>
 * <li>CP group itself is terminated</li>
 * <li>uncommitted entry count reaches to
 * (see {@link RaftAlgorithmConfig#getUncommittedEntryCountToRejectNewAppends()})</li>
 * <li>a membership change is requested before an entry is committed
 * on a term</li>
 * </ul>
 */
public class CannotReplicateException extends CPSubsystemException implements RetryableException {

    private static final long serialVersionUID = 4407025930140337716L;

    public CannotReplicateException(RaftEndpoint leader) {
        super("Cannot replicate new operations for now", leader != null ? leader.getUuid() : null);
    }

    private CannotReplicateException(UUID leaderUuid, Throwable cause) {
        super("Cannot replicate new operations for now", cause, leaderUuid);
    }

    @Override
    public CannotReplicateException wrap() {
        return new CannotReplicateException(getLeaderUuid(), this);
    }
}
