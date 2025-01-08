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

package com.hazelcast.cp.internal;

import com.hazelcast.cp.CPMember;

import java.util.Collection;
import java.util.Objects;

/**
 * Data class used for sharing Raft group information to clients via
 * {@link com.hazelcast.client.impl.protocol.codec.ClientAddCPGroupViewListenerCodec}.
 * CP to AP UUID mapping sent separately, to avoid duplicate entries between groups.
 */
public class RaftGroupInfo {
    private final RaftGroupId groupId;
    private final CPMember leader;
    private final Collection<CPMember> followers;

    public RaftGroupInfo(RaftGroupId groupId, CPMember leader, Collection<CPMember> followers) {
        this.groupId = groupId;
        this.leader = leader;
        this.followers = followers;
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }

    public CPMember getLeader() {
        return leader;
    }

    public Collection<CPMember> getFollowers() {
        return followers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RaftGroupInfo that = (RaftGroupInfo) o;
        return Objects.equals(groupId, that.groupId) && Objects.equals(leader, that.leader) && Objects.equals(followers,
                that.followers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, leader, followers);
    }

    @Override
    public String toString() {
        return "RaftGroupInfo{"
                + "groupId=" + groupId + ", "
                + "leader=" + leader + ", "
                + "followers=" + followers + ", "
                + "}";
    }
}
