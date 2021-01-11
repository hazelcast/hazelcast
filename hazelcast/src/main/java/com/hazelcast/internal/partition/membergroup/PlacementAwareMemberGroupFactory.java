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
            final String nodeInfo = member.getAttribute(PartitionGroupMetaData.PARTITION_GROUP_PLACEMENT);
            if (nodeInfo == null) {
                throw new IllegalArgumentException("Not enough metadata information is provided. "
                        + "A group name indicating the placement group must be provided with "
                        + "PLACEMENT_AWARE partition group.");
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
