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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * NodeAwareMemberGroupFactory is responsible for MemberGroups
 * creation according to the Kubernetes Node metadata provided by
 * {@link DiscoveryStrategy#discoverLocalMetadata()}
 * @since 3.7
 */
public class NodeAwareMemberGroupFactory extends BackupSafeMemberGroupFactory implements MemberGroupFactory {

    @Override
    protected Set<MemberGroup> createInternalMemberGroups(Collection<? extends Member> allMembers) {
        Map<String, MemberGroup> groups = createHashMap(allMembers.size());
        for (Member member : allMembers) {
            final String nodeInfo = member.getStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_NODE);
            if (nodeInfo == null) {
                throw new IllegalArgumentException("Not enough metadata information is provided. "
                        + "Kubernetes node name information must be provided with NODE_AWARE partition group.");
            }
            MemberGroup group = groups.get(nodeInfo);
            if (group == null) {
                group = new DefaultMemberGroup();
                groups.put(nodeInfo, group);
            }
            group.addMember(member);
        }
        return new HashSet<MemberGroup>(groups.values());
    }
}
