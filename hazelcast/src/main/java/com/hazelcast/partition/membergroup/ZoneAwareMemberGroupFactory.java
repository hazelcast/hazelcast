/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * ZoneAwareMemberGroupFactory is responsible for MemberGroups
 * creation according to the host metadata provided by
 * {@link DiscoveryStrategy#discoverLocalMetadata()}
 * @since 3.7
 */
public class ZoneAwareMemberGroupFactory extends BackupSafeMemberGroupFactory implements MemberGroupFactory {

    @Override
    protected Set<MemberGroup> createInternalMemberGroups(Collection<? extends Member> allMembers) {
        Map<String, MemberGroup> groups = new HashMap<String, MemberGroup>();
        for (Member member : allMembers) {
            final String zoneInfo = member.getStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_ZONE);
            if (zoneInfo == null) {
                throw new IllegalArgumentException("Not enough metadata information is provided. "
                        + "Availability zone information must be provided with ZONE_AWARE partition group.");
            }
            MemberGroup group = groups.get(zoneInfo);
            if (group == null) {
                group = new DefaultMemberGroup();
                groups.put(zoneInfo, group);
            }
            group.addMember(member);
        }
        return new HashSet<MemberGroup>(groups.values());
    }
}
