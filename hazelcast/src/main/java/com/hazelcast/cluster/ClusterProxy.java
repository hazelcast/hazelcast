/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.impl.MemberImpl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @mdogan 8/26/12
 */
public class ClusterProxy implements Cluster {

    private final ClusterService clusterService;

    public ClusterProxy(final ClusterService clusterService) {this.clusterService = clusterService;}

    public Member getLocalMember() {
        return clusterService.getLocalMember();
    }

    public Set<Member> getMembers() {
        final Collection<MemberImpl> members = clusterService.getMemberList();
        return members != null ? new HashSet<Member>(members) : new HashSet<Member>(0);
    }

    public long getClusterTime() {
        return clusterService.getClusterTime();
    }

    public void addMembershipListener(MembershipListener listener) {
        clusterService.addMembershipListener(listener);
    }

    public void removeMembershipListener(MembershipListener listener) {
        clusterService.removeMembershipListener(listener);
    }
}
