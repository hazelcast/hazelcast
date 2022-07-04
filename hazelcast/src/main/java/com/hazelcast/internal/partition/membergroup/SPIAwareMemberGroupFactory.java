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


import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.properties.ValidationException;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.partitiongroup.MemberGroup;
import com.hazelcast.spi.partitiongroup.PartitionGroupStrategy;

import java.util.Collection;
import java.util.Set;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * SPIAwareMemberGroupFactory is responsible for providing custom MemberGroups
 * implemented by the user in {@link DiscoveryStrategy#getPartitionGroupStrategy()}
 * to the {@link PartitionStateManager}.
 *
 * @since 3.7
 */

public class SPIAwareMemberGroupFactory extends BackupSafeMemberGroupFactory implements MemberGroupFactory {

    private final DiscoveryService discoveryService;

    public SPIAwareMemberGroupFactory(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    @Override
    protected Set<MemberGroup> createInternalMemberGroups(Collection<? extends Member> allMembers) {
        Set<MemberGroup> memberGroups = createHashSet(allMembers.size());

        for (Member member : allMembers) {
            try {
                if (member.localMember()) {
                    DefaultDiscoveryService defaultDiscoveryService = (DefaultDiscoveryService) discoveryService;
                    // If no discovery strategy is found fail-fast
                    if (!defaultDiscoveryService.getDiscoveryStrategies().iterator().hasNext()) {
                        throw new RuntimeException("Could not load any Discovery Strategy, please "
                                + "check service definitions under META_INF.services folder. ");
                    } else {
                        for (DiscoveryStrategy discoveryStrategy : defaultDiscoveryService.getDiscoveryStrategies()) {
                            PartitionGroupStrategy groupStrategy = discoveryStrategy.getPartitionGroupStrategy(allMembers);
                            if (groupStrategy == null) {
                                groupStrategy = discoveryStrategy.getPartitionGroupStrategy();
                            }
                            checkNotNull(groupStrategy);
                            for (MemberGroup group : groupStrategy.getMemberGroups()) {
                                memberGroups.add(group);
                            }
                            return memberGroups;
                        }
                    }
                }
            } catch (Exception e) {
                if (e instanceof ValidationException) {
                    throw new InvalidConfigurationException("Invalid configuration", e);
                } else {
                    throw new RuntimeException("Failed to configure discovery strategies", e);
                }
            }
        }

        return memberGroups;
    }

}
