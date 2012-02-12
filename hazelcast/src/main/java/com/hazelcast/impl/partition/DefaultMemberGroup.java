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

import com.hazelcast.impl.MemberImpl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DefaultMemberGroup implements MemberGroup {

    private final Set<MemberImpl> members = new HashSet<MemberImpl>();

    public DefaultMemberGroup() {
        super();
    }

    public DefaultMemberGroup(Collection<MemberImpl> members) {
        super();
        addMembers(members);
    }

    public void addMember(MemberImpl member) {
        members.add(member);
    }

    public void addMembers(Collection<MemberImpl> members) {
        this.members.addAll(members);
    }

    public void removeMember(MemberImpl member) {
        members.remove(member);
    }

    public boolean hasMember(MemberImpl member) {
        return members.contains(member);
    }

    public Set<MemberImpl> getMembers() {
        return members;
    }

    public Iterator<MemberImpl> iterator() {
        return members.iterator();
    }

    public int size() {
        return members.size();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((members == null) ? 0 : members.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DefaultMemberGroup other = (DefaultMemberGroup) obj;
        if (members == null) {
            if (other.members != null)
                return false;
        } else if (!members.equals(other.members))
            return false;
        return true;
    }
}
