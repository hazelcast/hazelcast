/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.impl;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The RandomLB randomly selects a member to route to.
 */
public class RandomLB implements LoadBalancer, MembershipListener {
    final AtomicReference<List<Member>> membersRef = new AtomicReference<List<Member>>();
    final Random random = new Random(System.currentTimeMillis());

    @Override
    public void init(HazelcastInstance h, ClientConfig config) {
        Cluster cluster = h.getCluster();
        cluster.addMembershipListener(this);
        List<Member> members = new LinkedList<Member>();
        members.addAll(cluster.getMembers());
        membersRef.set(members);
    }

    @Override
    public Member next() {
        List<Member> members = membersRef.get();
        if(members.isEmpty()){
            return null;
        }
        int index = random.nextInt(members.size());
        return members.get(index);
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        List<Member> newMembers = new LinkedList<Member>(membersRef.get());
        newMembers.add(membershipEvent.getMember());
        membersRef.set(newMembers);
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        List<Member> newMembers = new LinkedList<Member>(membersRef.get());
        newMembers.remove(membershipEvent.getMember());
        membersRef.set(newMembers);
    }
}