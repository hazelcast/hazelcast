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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.hazelcast.monitor.DistributedMemberInfoCallable;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.MemberInfo;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.hazelcast.monitor.server.event.MembershipEventGenerator.getName;

public class MemberInfoEventGenerator implements ChangeEventGenerator {
    private int clusterId;
    private HazelcastClient client;
    private Member member;

    public MemberInfoEventGenerator(HazelcastClient client, int clusterId, String member) {
        this.client = client;
        this.clusterId = clusterId;
        Set<Member> members = client.getCluster().getMembers();
        for (Member m : members) {
            String name = getName(m);
            if (name.equals(member)) {
                this.member = m;
            }
        }
    }

    public ChangeEvent generateEvent() {
        if (member == null) {
            return null;
        }
        ExecutorService esService = client.getExecutorService();
        DistributedTask<DistributedMemberInfoCallable.MemberInfo> task =
                new DistributedTask<DistributedMemberInfoCallable.MemberInfo>(new DistributedMemberInfoCallable(), member);
        esService.submit(task);
        DistributedMemberInfoCallable.MemberInfo result;
        try {
            result = task.get();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        MemberInfo memberInfo = convert(result);
        return memberInfo;
    }

    private MemberInfo convert(DistributedMemberInfoCallable.MemberInfo memberInfo) {
        if (memberInfo == null) {
            return null;
        }
        MemberInfo info = new MemberInfo();
        info.setAvailableProcessors(memberInfo.getAvailableProcessors());
        info.setFreeMemory(memberInfo.getFreeMemory());
        info.setMaxMemory(memberInfo.getMaxMemory());
        info.setTotalMemory(memberInfo.getTotalMemory());
        info.setPartitions(memberInfo.getPartitions());
        info.setTime(memberInfo.getTime());
        info.addSystemProps(memberInfo.getSystemProps());
        return info;
    }

    public ChangeEventType getChangeEventType() {
        return ChangeEventType.MEMBER_INFO;
    }

    public int getClusterId() {
        return clusterId;
    }
}
