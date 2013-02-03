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

package com.hazelcast.impl.partition;

import com.hazelcast.impl.MemberImpl;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

abstract class BackupSafeMemberGroupFactory implements MemberGroupFactory {

    public final Collection<MemberGroup> createMemberGroups(final Collection<MemberImpl> allMembers) {
        final Collection<MemberImpl> members = removeLiteMembers(allMembers);
        final Collection<MemberGroup> groups = createInternalMemberGroups(members);
        if (groups.size() == 1 && members.size() > 1) {
            // If there are more than one members and just one group
            // then split members into two groups to guarantee at least the first backup.
            MemberGroup group1 = groups.iterator().next();
            MemberGroup group2 = new DefaultMemberGroup();
            final int sizePerGroup = group1.size() / 2;

            final Iterator<MemberImpl> iter = group1.iterator();
            while (group2.size() < sizePerGroup && iter.hasNext()) {
                group2.addMember(iter.next());
                iter.remove();
            }
            groups.add(group2);
        }
        return groups;
    }

    protected abstract Set<MemberGroup> createInternalMemberGroups(final Collection<MemberImpl> allMembers) ;

    private Collection<MemberImpl> removeLiteMembers(Collection<MemberImpl> members) {
        final Collection<MemberImpl> result = new LinkedList<MemberImpl>();
        for (MemberImpl member : members) {
            if (!member.isLiteMember()) {
                result.add(member);
            }
        }
        return result;
    }

}
