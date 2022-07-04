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

import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

public final class MemberGroupFactoryFactory {

    private MemberGroupFactoryFactory() {
    }

    public static MemberGroupFactory newMemberGroupFactory(PartitionGroupConfig partitionGroupConfig,
                                                           DiscoveryService discoveryService) {
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
            case ZONE_AWARE:
                return new ZoneAwareMemberGroupFactory();
            case NODE_AWARE:
                return new NodeAwareMemberGroupFactory();
            case PLACEMENT_AWARE:
                return new PlacementAwareMemberGroupFactory();
            case SPI:
                return new SPIAwareMemberGroupFactory(discoveryService);
            default:
                throw new RuntimeException("Unknown MemberGroupType:" + memberGroupType);
        }
    }
}
