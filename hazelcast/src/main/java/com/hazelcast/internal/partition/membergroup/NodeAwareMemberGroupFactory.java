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
 * NodeAwareMemberGroupFactory is responsible for MemberGroups
 * creation according to name of the node metadata. For container orchestration
 * tools like Kubernetes and Docker Swarm, node is the term used to refer
 * machine that containers/pods run on. A node may be a virtual or physical machine.
 * Node name metadata provided by
 * {@link DiscoveryStrategy#discoverLocalMetadata()}
 */
public class NodeAwareMemberGroupFactory extends BackupSafeMemberGroupFactory implements MemberGroupFactory {

    @Override
    protected Set<MemberGroup> createInternalMemberGroups(Collection<? extends Member> allMembers) {
        Map<String, MemberGroup> groups = createHashMap(allMembers.size());
        for (Member member : allMembers) {
            final String nodeInfo = member.getAttribute(PartitionGroupMetaData.PARTITION_GROUP_NODE);
            if (nodeInfo == null) {
                throw new IllegalArgumentException("Not enough metadata information is provided. "
                        + "Node name information must be provided with NODE_AWARE partition group.");
            }
            MemberGroup group = groups.get(nodeInfo);
            if (group == null) {
                group = new DefaultMemberGroup();
                groups.put(nodeInfo, group);
            }
            group.addMember(member);
        }
        return new HashSet<>(groups.values());
    }
}
