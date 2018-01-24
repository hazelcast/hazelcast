/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.raft;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftMetadataGroupConfig {

    /**
     * Size of the metadata Raft group. If not specified explicitly, then all Raft members will be in metadata group.
     */
    private int metadataGroupSize;

    /**
     * Raft members. Members of a Raft group are selected among these pre-defined members.
     * First {@link #metadataGroupSize} of these will be members of the metadata Raft group.
     */
    private final List<RaftMember> members = new ArrayList<RaftMember>();

    public RaftMetadataGroupConfig() {
    }

    public RaftMetadataGroupConfig(RaftMetadataGroupConfig config) {
        this.metadataGroupSize = config.metadataGroupSize;
        for (RaftMember member : config.members) {
            members.add(new RaftMember(member));
        }
    }

    public List<RaftMember> getMembers() {
        return members;
    }

    public RaftMetadataGroupConfig setMembers(List<RaftMember> m) {
        checkTrue(m.size() > 1, "Raft groups must have at least 2 members");

        members.clear();
        for (RaftMember member : m) {
            if (!members.contains(member)) {
                members.add(member);
            }
        }
        return this;
    }

    public int getMetadataGroupSize() {
        return metadataGroupSize;
    }

    public RaftMetadataGroupConfig setMetadataGroupSize(int metadataGroupSize) {
        checkTrue(metadataGroupSize >= 2, "The metadata group must have at least 2 members");
        checkTrue(metadataGroupSize <= members.size(),
                "The metadata group cannot be bigger than the number of raft members");
        this.metadataGroupSize = metadataGroupSize;

        return this;
    }

    public List<RaftMember> getMetadataGroupMembers() {
        if (metadataGroupSize == 0) {
            metadataGroupSize = members.size();
        }
        return members.subList(0, metadataGroupSize);
    }

}
