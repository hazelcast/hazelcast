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

package com.hazelcast.raft.exception;

import com.hazelcast.config.raft.RaftAlgorithmConfig;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.spi.exception.RetryableException;

/**
 * A {@code RaftException} which is thrown when an entry cannot be replicated.
 * An append request can be rejected when,
 * <ul>
 * <li>a member leaves the Raft group</li>
 * <li>Raft group itself is terminated</li>
 * <li>uncommitted entry count reaches to max (see {@link RaftAlgorithmConfig#uncommittedEntryCountToRejectNewAppends})</li>
 * <li>a membership change is requested before an entry is committed on a term</li>
 * </ul>
 */
public class CannotReplicateException extends RaftException implements RetryableException {

    public CannotReplicateException(RaftMember leader) {
        super("Cannot replicate new operations for now", leader);
    }
}
