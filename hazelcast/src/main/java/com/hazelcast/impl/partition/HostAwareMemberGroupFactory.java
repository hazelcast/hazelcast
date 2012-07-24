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

package com.hazelcast.impl.partition;

import com.hazelcast.core.Member;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.nio.Address;

import java.util.*;

public class HostAwareMemberGroupFactory implements MemberGroupFactory {

    public Collection<MemberGroup> createMemberGroups(final Collection<Member> allMembers) {
        final Collection<Member> members = removeLiteMembers(allMembers);
        final Collection<MemberGroup> groups = createHostAwareMemberGroups(members);
        if (groups.size() == 1 && members.size() >= 2) {
            // If there are multiple members and just one host
            // then split members into two groups to guarantee first backup.
            MemberGroup group1 = groups.iterator().next();
            MemberGroup group2 = new DefaultMemberGroup();
            final int sizePerGroup = group1.size() / 2;

            Iterator<Member> iter = group1.iterator();
            while (group2.size() < sizePerGroup && iter.hasNext()) {
                group2.addMember(iter.next());
                iter.remove();
            }
            groups.add(group2);
        }
        return groups;
    }

    private Collection<MemberGroup> createHostAwareMemberGroups(final Collection<Member> members) {
        Map<String, MemberGroup> groups = new HashMap<String, MemberGroup>();
        for (Member member : members) {
            if (!member.isLiteMember()) {
                Address address = ((MemberImpl) member).getAddress();
                MemberGroup group = groups.get(address.getHost());
                if (group == null) {
                    group = new DefaultMemberGroup();
                    groups.put(address.getHost(), group);
                }
                group.addMember(member);
            }
        }
        return new HashSet<MemberGroup>(groups.values());
    }

    private Collection<Member> removeLiteMembers(Collection<Member> members) {
        final Collection<Member> result = new LinkedList<Member>();
        for (Member member : members) {
            if (!member.isLiteMember()) {
                result.add(member);
            }
        }
        return result;
    }
}
