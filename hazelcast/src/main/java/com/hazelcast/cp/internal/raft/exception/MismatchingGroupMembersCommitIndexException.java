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

package com.hazelcast.cp.internal.raft.exception;

import com.hazelcast.cp.exception.CPSubsystemException;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;

import java.util.Collection;

/**
 * A {@code CPSubsystemException} which is thrown when a membership change is
 * requested but expected members commitIndex doesn't match the actual members
 * commitIndex in the Raft state.
 * Handled internally.
 */
public class MismatchingGroupMembersCommitIndexException extends CPSubsystemException {

    private static final long serialVersionUID = -109570074579015635L;

    private final long commitIndex;

    private final Collection<RaftEndpoint> members;

    public MismatchingGroupMembersCommitIndexException(long commitIndex, Collection<RaftEndpoint> members) {
        super("commit index: " + commitIndex + " members: " + members, null);
        this.commitIndex = commitIndex;
        this.members = members;
    }

    private MismatchingGroupMembersCommitIndexException(long commitIndex, Collection<RaftEndpoint> members, Throwable cause) {
        super("commit index: " + commitIndex + " members: " + members, cause, null);
        this.commitIndex = commitIndex;
        this.members = members;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public Collection<RaftEndpoint> getMembers() {
        return members;
    }

    @Override
    public MismatchingGroupMembersCommitIndexException wrap() {
        return new MismatchingGroupMembersCommitIndexException(commitIndex, members, this);
    }
}
