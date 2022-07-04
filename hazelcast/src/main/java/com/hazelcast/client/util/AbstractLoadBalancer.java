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

package com.hazelcast.client.util;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An abstract {@link LoadBalancer} implementation.
 */
public abstract class AbstractLoadBalancer implements LoadBalancer, InitialMembershipListener {

    private static final Member[] EMPTY_MEMBERS = new Member[0];

    private final AtomicReference<Members> membersRef = new AtomicReference<>(new Members(EMPTY_MEMBERS, EMPTY_MEMBERS));
    private volatile Cluster clusterRef;

    @Override
    public final void init(Cluster cluster, ClientConfig config) {
        this.clusterRef = cluster;
        cluster.addMembershipListener(this);
    }

    private void setMembersRef() {
        Set<Member> memberSet = clusterRef.getMembers();

        Member[] members = memberSet.toArray(new Member[0]);
        Member[] dataMembers = dataMembers(members);

        membersRef.set(new Members(members, dataMembers));
    }

    protected Member[] getMembers() {
        return membersRef.get().getMembers();
    }

    protected Member[] getDataMembers() {
        return membersRef.get().getDataMembers();
    }

    @Override
    public final void init(InitialMembershipEvent event) {
        setMembersRef();
    }

    @Override
    public final void memberAdded(MembershipEvent membershipEvent) {
        setMembersRef();
    }

    @Override
    public final void memberRemoved(MembershipEvent membershipEvent) {
        setMembersRef();
    }

    private static Member[] dataMembers(Member[] members) {
        List<Member> dataMembers = new ArrayList<>(members.length);

        for (Member member : members) {
            if (!member.isLiteMember()) {
                dataMembers.add(member);
            }
        }

        if (dataMembers.isEmpty()) {
            return EMPTY_MEMBERS;
        }

        return dataMembers.toArray(new Member[0]);
    }

    private static final class Members {

        private final Member[] members;
        private final Member[] dataMembers;

        private Members(Member[] members, Member[] dataMembers) {
            this.members = members;
            this.dataMembers = dataMembers;
        }

        private Member[] getMembers() {
            return members;
        }

        private Member[] getDataMembers() {
            return dataMembers;
        }
    }
}
