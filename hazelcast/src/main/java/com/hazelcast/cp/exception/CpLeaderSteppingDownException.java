/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.spi.exception.RetryableException;

import java.io.Serial;
import java.util.UUID;

/**
 * This exception is thrown when a replication request is rejected by
 * a leader due to being configured to auto step down when leader.
 * see {@link com.hazelcast.config.cp.CPSubsystemConfig#isAutoStepDownWhenLeader()}.
 * @since 5.7
 */
public class CpLeaderSteppingDownException extends CPSubsystemException implements RetryableException {

    @Serial
    private static final long serialVersionUID = -6750771858377660079L;

    public CpLeaderSteppingDownException(RaftEndpoint leader) {
        super("Service unavailable, leader is auto stepping down", leader != null ? leader.getUuid() : null);
    }

    public CpLeaderSteppingDownException(String message, UUID leaderUuid) {
        super(message, leaderUuid);
    }

    public CpLeaderSteppingDownException(String message, UUID leaderUuid, Throwable cause) {
        super(message, cause, leaderUuid);
    }

    @Override
    public CpLeaderSteppingDownException wrap() {
        return new CpLeaderSteppingDownException(getMessage(), getLeaderUuid(), this);
    }
}
