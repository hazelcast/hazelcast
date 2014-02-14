/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An abstract {@link com.hazelcast.client.LoadBalancer} implementation.
 */
public abstract class AbstractLoadBalancer implements LoadBalancer, MembershipListener {

    private final AtomicReference<Member[]> membersRef = new AtomicReference(new Member[]{});
    private volatile Cluster clusterRef;

    @Override
    public final void init(Cluster cluster, ClientConfig config) {
        this.clusterRef = cluster;
        setMembersRef();
        cluster.addMembershipListener(this);
    }

    private void setMembersRef() {
        Cluster cluster = clusterRef;
        if (cluster != null) {
            Set<Member> memberSet = cluster.getMembers();
            Member[] members = memberSet.toArray(new Member[memberSet.size()]);
            membersRef.set(members);
        }
    }

    protected Member[] getMembers() {
        return membersRef.get();
    }

    @Override
    public final void memberAdded(MembershipEvent membershipEvent) {
        setMembersRef();
    }

    @Override
    public final void memberRemoved(MembershipEvent membershipEvent) {
        setMembersRef();
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
    }
}
