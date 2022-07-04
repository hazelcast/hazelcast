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
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.partitiongroup.MemberGroup;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * PlacementAwareMemberGroupFactory is responsible for MemberGroups
 * creation according to placement group metadata. Placement groups are
 * logical or physical groups of VMs based on their racks, power sources,
 * network, resources, etc. assigned by cloud providers. It provides good
 * redundancy when running members within a single availability zone.
 * Member groups are formed based on the metadata provided by
 * {@link DiscoveryStrategy#discoverLocalMetadata()}
 */
public class PlacementAwareMemberGroupFactory extends BackupSafeMemberGroupFactory implements MemberGroupFactory {

    @Override
    protected Set<MemberGroup> createInternalMemberGroups(Collection<? extends Member> allMembers) {
        Map<String, MemberGroup> groups = createHashMap(allMembers.size());
        for (Member member : allMembers) {
            String placementInfo = member.getAttribute(PartitionGroupMetaData.PARTITION_GROUP_PLACEMENT);
            if (placementInfo == null) {
                throw new IllegalArgumentException("Not enough metadata information is provided. "
                        + "A group name indicating the placement group must be provided with "
                        + "PLACEMENT_AWARE partition group.");
            }
            MemberGroup group = groups.get(placementInfo);
            if (group == null) {
                group = new DefaultMemberGroup();
                groups.put(placementInfo, group);
            }
            group.addMember(member);
        }
        return new HashSet<>(groups.values());
    }

}
