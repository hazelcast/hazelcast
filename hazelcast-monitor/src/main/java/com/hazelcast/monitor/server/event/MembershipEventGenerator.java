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

package com.hazelcast.monitor.server.event;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.MemberEvent;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class MembershipEventGenerator implements ChangeEventGenerator {
    private HazelcastInstance hazelcastInstance;
    final private int clusterId;

    public MembershipEventGenerator(HazelcastInstance hazelcastInstance, int clusterId) {
        this.hazelcastInstance = hazelcastInstance;
        this.clusterId = clusterId;
    }

    public ChangeEvent generateEvent() {
//        System.out.println("Generating event for cluster id: "+clusterId  + ": "+this.hashCode());
        Set<Member> members = hazelcastInstance.getCluster().getMembers();
        List<String> memberList = new ArrayList<String>();
        for (Iterator<Member> iterator = members.iterator(); iterator.hasNext();) {
            Member member = iterator.next();
            String memberName = getName(member);
            memberList.add(memberName);
        }
        MemberEvent memberEvent = new MemberEvent(clusterId);
        memberEvent.setMembers(memberList);
        return memberEvent;
    }

    public static String getName(Member member) {
        InetSocketAddress socketAddress = member.getInetSocketAddress();
        String memberName = null;
        if (socketAddress != null) {
            memberName = socketAddress.getHostName() + ":" + socketAddress.getPort();
        }
        return memberName;
    }

    public ChangeEventType getChangeEventType() {
        return ChangeEventType.MEMBER_EVENT;
    }

    public int getClusterId() {
        return clusterId;
    }
}
