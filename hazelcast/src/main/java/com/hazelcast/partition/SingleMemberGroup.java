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

import java.util.Collection;
import java.util.Iterator;

public class SingleMemberGroup implements MemberGroup {

    private Member member;

    public SingleMemberGroup() {
        super();
    }

    public SingleMemberGroup(Member member) {
        super();
        this.member = member;
    }

    public void addMember(Member member) {
        if (this.member != null) {
            throw new UnsupportedOperationException();
        }
        this.member = member;
    }

    public void addMembers(Collection<Member> members) {
        throw new UnsupportedOperationException();
    }

    public void removeMember(Member member) {
        if (this.member != null && this.member.equals(member)) {
            this.member = null;
        }
    }

    public boolean hasMember(Member member) {
        return this.member != null && this.member.equals(member);
    }

    public Iterator<Member> iterator() {
        return new MemberIterator();
    }

    public int size() {
        return member != null ? 1 : 0;
    }

    private class MemberIterator implements Iterator<Member> {
        boolean end = false;

        public boolean hasNext() {
            return !end;
        }

        public Member next() {
            if (hasNext()) {
                end = true;
                return member;
            }
            return null;
        }

        public void remove() {
            if (end) {
                member = null;
            }
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((member == null) ? 0 : member.hashCode());
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
        SingleMemberGroup other = (SingleMemberGroup) obj;
        if (member == null) {
            if (other.member != null)
                return false;
        } else if (!member.equals(other.member))
            return false;
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("SingleMemberGroup");
        sb.append("{member=").append(member);
        sb.append('}');
        return sb.toString();
    }
}
