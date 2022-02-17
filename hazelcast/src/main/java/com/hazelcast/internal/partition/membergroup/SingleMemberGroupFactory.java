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
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * Arranges members in single-member groups (every member is its own group).
 */
public class SingleMemberGroupFactory implements MemberGroupFactory {

    @Override
    public Set<MemberGroup> createMemberGroups(Collection<? extends Member> members) {
        Set<MemberGroup> groups = createHashSet(members.size());
        for (Member member : members) {
            groups.add(new SingleMemberGroup(member));
        }
        return groups;
    }
}
