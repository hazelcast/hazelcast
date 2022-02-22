/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.membergroup;

import com.hazelcast.cluster.Member;
import com.hazelcast.spi.partitiongroup.MemberGroup;

import java.util.Collection;
import java.util.Iterator;

public class SingleMemberGroup implements MemberGroup {

    private Member member;

    public SingleMemberGroup() {
    }

    public SingleMemberGroup(Member member) {
        this.member = member;
    }

    @Override
    public void addMember(Member member) {
        if (this.member != null) {
            throw new UnsupportedOperationException();
        }
        this.member = member;
    }

    @Override
    public void addMembers(Collection<Member> members) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeMember(Member member) {
        if (this.member != null && this.member.equals(member)) {
            this.member = null;
        }
    }

    @Override
    public boolean hasMember(Member member) {
        return this.member != null && this.member.equals(member);
    }

    @Override
    public Iterator<Member> iterator() {
        return new MemberIterator();
    }

    @Override
    public int size() {
        return member != null ? 1 : 0;
    }

    private class MemberIterator implements Iterator<Member> {
        boolean end;

        @Override
        public boolean hasNext() {
            return !end;
        }

        @Override
        public Member next() {
            if (hasNext()) {
                end = true;
                return member;
            }
            return null;
        }

        @Override
        public void remove() {
            if (end) {
                member = null;
            }
        }
    }

    @Override
    public int hashCode() {
        int prime = 31;
        int result = 1;
        result = prime * result + ((member == null) ? 0 : member.hashCode());
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
        SingleMemberGroup other = (SingleMemberGroup) obj;
        if (member == null) {
            if (other.member != null) {
                return false;
            }
        } else if (!member.equals(other.member)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "SingleMemberGroup{member=" + member + '}';
    }
}
