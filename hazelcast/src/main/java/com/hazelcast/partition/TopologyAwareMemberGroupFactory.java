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

package com.hazelcast.partition;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;

import java.util.*;

public class TopologyAwareMemberGroupFactory extends BackupSafeMemberGroupFactory implements MemberGroupFactory {

    protected Set<MemberGroup> createInternalMemberGroups(final Collection<Member> allMembers) {
        final Map<String, MemberGroup> groups = new HashMap<String, MemberGroup>();
        Map<String, MemberGroup> hostsMaps = new HashMap<String, MemberGroup>();

        //Firstly try and create MemberGroups by site
        for (Member member : allMembers) {
            Address address = ((MemberImpl) member).getAddress();

            if (address.getSite() != null) {
                MemberGroup group = groups.get("Site:" + address.getSite());
                if (group == null) {
                    group = new DefaultMemberGroup();
                    groups.put("Site:" + address.getSite(), group);
                }
                group.addMember(member);
            }
            else if (address.getRack() != null) {
                MemberGroup group = groups.get("Rack:" + address.getRack());
                if (group == null) {
                    group = new DefaultMemberGroup();
                    groups.put("Rack:" + address.getRack(), group);
                }
                group.addMember(member);
            }
            else {

                MemberGroup group = hostsMaps.get("Host:" + address.getHost());
                if (group == null) {
                    group = new DefaultMemberGroup();
                    hostsMaps.put("Host:" + address.getHost(), group);
                }
                group.addMember(member);
            }

        }

        //If there is only one entry in the hosts map,
        //then we can potentially create a a number of groups if there
        //are members running on different processes (JVMs). This
        //creates a node safe configuration
        //Then see if it can actually be grouped by process id
        if (hostsMaps.size() == 1) {
            Iterator<Member> members = hostsMaps.entrySet().iterator().next().getValue().iterator();
            Map<String, MemberGroup> processesMap = new HashMap<String, MemberGroup>();
            while (members.hasNext()) {
                Member member = members.next();
                Address address = ((MemberImpl) member).getAddress();
                MemberGroup group = processesMap.get("Process:" + address.getProcess());
                if (group == null) {
                    group = new DefaultMemberGroup();
                    processesMap.put("Process:" + address.getProcess(), group);
                }
                group.addMember(member);
            }
            groups.putAll(processesMap);
        } else {
            groups.putAll(hostsMaps);
        }
        return new HashSet<MemberGroup>(groups.values());
    }
}

