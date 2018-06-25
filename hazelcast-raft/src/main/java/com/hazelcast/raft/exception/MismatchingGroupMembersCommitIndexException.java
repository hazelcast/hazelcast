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

import com.hazelcast.raft.RaftMember;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashSet;

/**
 * A {@code RaftException} which is thrown when a membership change is requested
 * but expected members commitIndex doesn't match the actual members commitIndex in the Raft state.
 */
public class MismatchingGroupMembersCommitIndexException extends RaftException {

    private transient long commitIndex;

    private transient Collection<RaftMember> members;

    public MismatchingGroupMembersCommitIndexException(long commitIndex, Collection<RaftMember> members) {
        super(null);
        this.commitIndex = commitIndex;
        this.members = members;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public Collection<RaftMember> getMembers() {
        return members;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeLong(commitIndex);
        out.writeInt(members.size());
        for (RaftMember endpoint : members) {
            out.writeObject(endpoint);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        commitIndex = in.readLong();
        int count = in.readInt();
        members = new HashSet<RaftMember>(count);
        for (int i = 0; i < count; i++) {
            members.add((RaftMember) in.readObject());
        }
    }
}
