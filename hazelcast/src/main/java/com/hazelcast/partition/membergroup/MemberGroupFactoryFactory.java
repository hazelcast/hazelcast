package com.hazelcast.partition.membergroup;

import com.hazelcast.config.PartitionGroupConfig;

public final class MemberGroupFactoryFactory {

    private MemberGroupFactoryFactory() {
    }

    public static MemberGroupFactory newMemberGroupFactory(PartitionGroupConfig partitionGroupConfig) {
        PartitionGroupConfig.MemberGroupType memberGroupType;

        if (partitionGroupConfig == null || !partitionGroupConfig.isEnabled()) {
            memberGroupType = PartitionGroupConfig.MemberGroupType.PER_MEMBER;
        } else {
            memberGroupType = partitionGroupConfig.getGroupType();
        }

        switch (memberGroupType) {
            case HOST_AWARE:
                return new HostAwareMemberGroupFactory();
            case CUSTOM:
                return new ConfigMemberGroupFactory(partitionGroupConfig.getMemberGroupConfigs());
            case PER_MEMBER:
                return new SingleMemberGroupFactory();
            default:
                throw new RuntimeException("Unknown MemberGroupType:" + memberGroupType);
        }
    }
}
