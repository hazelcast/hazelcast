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

    public static final String HAZELCAST_ADDRESS_SITE = "hazelcast.address.site";
    public static final String HAZELCAST_ADDRESS_RACK = "hazelcast.address.rack";
    public static final String HAZELCAST_ADDRESS_HOST = "hazelcast.address.host";
    public static final String HAZELCAST_ADDRESS_PROCESS = "hazelcast.address.process";

    protected Set<MemberGroup> createInternalMemberGroups(final Collection<Member> allMembers) {
        final Map<String, MemberGroup> groups = new HashMap<String, MemberGroup>();
        Map<String, MemberGroup> hostsMaps = new HashMap<String, MemberGroup>();


        for (Member member : allMembers) {
            Address address = ((MemberImpl) member).getAddress();

            String siteAddress = null;
            String rackAddress = null;
            String hostAddress = null;
            if (null != member.getAttributes()) {
                siteAddress = (null == member.getAttributes()) ? null : (String)member.getAttributes().get(HAZELCAST_ADDRESS_SITE);
                rackAddress = (null == member.getAttributes()) ? null : (String)member.getAttributes().get(HAZELCAST_ADDRESS_RACK);
                hostAddress = (null == member.getAttributes()) ? null : (String)member.getAttributes().get(HAZELCAST_ADDRESS_HOST);
            }


            //Try and create MemberGroups by site
            if (null != siteAddress) {
                MemberGroup group = groups.get("Site:" + siteAddress);
                if (group == null) {
                    group = new DefaultMemberGroup();
                    groups.put("Site:" + siteAddress, group);
                }
                group.addMember(member);
            }
            //Try and create MemberGroups by rack
            else if (null != rackAddress) {
                MemberGroup group = groups.get("Rack:" + rackAddress);
                if (group == null) {
                    group = new DefaultMemberGroup();
                    groups.put("Rack:" + rackAddress, group);
                }
                group.addMember(member);
            }
            //Try and create MemberGroups by host
            else {
                //Lets try and get the HOST member attribute first. If that fails fall back to the IP address. We use
                //host member attribute first as this should be common to the host, where as a single host can actually
                //have multiple IP addresses.
                if (null == hostAddress) {
                    hostAddress = address.getHost();
                }
                MemberGroup group = hostsMaps.get("Host:" + hostAddress);
                if (group == null) {
                    group = new DefaultMemberGroup();
                    hostsMaps.put("Host:" + hostAddress, group);
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
                String processAddress = (null == member.getAttributes()) ? null : (String)member.getAttributes().get(HAZELCAST_ADDRESS_PROCESS);
                if (processAddress != null) {
                    MemberGroup group = processesMap.get("Process:" + processAddress);
                    if (group == null) {
                        group = new DefaultMemberGroup();
                        processesMap.put("Process:" + processAddress, group);
                    }
                    group.addMember(member);
                }
            }
            groups.putAll(processesMap);
        } else {
            groups.putAll(hostsMaps);
        }

        return new HashSet<MemberGroup>(groups.values());
    }
}

