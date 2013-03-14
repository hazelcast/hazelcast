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

import com.hazelcast.client.Router;
import com.hazelcast.core.*;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RoundRobinRouter implements Router, MembershipListener {
    final AtomicLong index = new AtomicLong(0);
    final AtomicReference<Member[]> memberRef = new AtomicReference(new Member[]{});

    @Override
    public void init(HazelcastInstance h) {
        Cluster cluster = h.getCluster();
        for (Member member : cluster.getMembers()) {
            addMember(member);
        }
        cluster.addMembershipListener(this);
    }

    @Override
    public Member next() {
        Member[] members = memberRef.get();
        if (members.length == 0) return null;
        Member member = members[(int) (index.getAndAdd(1) % members.length)];
        return member;
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        Member member = membershipEvent.getMember();
        addMember(member);
    }

    private void addMember(Member member) {
        Member[] oldList = memberRef.get();
        Member[] newList = new Member[oldList.length + 1];
        System.arraycopy(oldList, 0, newList, 0, oldList.length);
        newList[oldList.length] = member;
        memberRef.compareAndSet(oldList, newList);
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        Member member = membershipEvent.getMember();
        Member[] oldList = memberRef.get();
        int i = Arrays.binarySearch(oldList, member, new Comparator<Member>() {
            @Override
            public int compare(Member o1, Member o2) {
                return o1.getInetSocketAddress().equals(o2.getInetSocketAddress()) ? 0 : -1;
            }
        });
        Member[] newList = new Member[oldList.length - 1];
        System.arraycopy(oldList, 0, newList, 0, i);
        System.arraycopy(oldList, i + 1, newList, i, oldList.length - i - 1);
        memberRef.compareAndSet(oldList, newList);
    }
}
