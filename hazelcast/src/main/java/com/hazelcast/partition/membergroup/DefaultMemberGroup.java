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

package com.hazelcast.partition.membergroup;

import com.hazelcast.core.Member;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DefaultMemberGroup implements MemberGroup {

    private final Set<Member> members = new HashSet<Member>();

    public DefaultMemberGroup() {
    }

    public DefaultMemberGroup(Collection<Member> members) {
        addMembers(members);
    }

    @Override
    public void addMember(Member member) {
        members.add(member);
    }

    @Override
    public void addMembers(Collection<Member> members) {
        this.members.addAll(members);
    }

    @Override
    public void removeMember(Member member) {
        members.remove(member);
    }

    @Override
    public boolean hasMember(Member member) {
        return members.contains(member);
    }

    public Set<Member> getMembers() {
        return members;
    }

    @Override
    public Iterator<Member> iterator() {
        return members.iterator();
    }

    @Override
    public int size() {
        return members.size();
    }

    @Override
    public int hashCode() {
        int prime = 31;
        int result = 1;
        result = prime * result + (members.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DefaultMemberGroup other = (DefaultMemberGroup) obj;
        return members.equals(other.members);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DefaultMemberGroup");
        sb.append("{members=").append(members);
        sb.append('}');
        return sb.toString();
    }
}
