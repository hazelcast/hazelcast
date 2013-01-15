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

import com.hazelcast.config.PartitionGroupConfig;

public class PartitionStateGeneratorFactory {

    public static PartitionStateGenerator newRandomPartitionStateGenerator() {
        return new PartitionStateGeneratorImpl(new SingleMemberGroupFactory());
    }

    public static PartitionStateGenerator newHostAwarePartitionStateGenerator() {
        return new PartitionStateGeneratorImpl(new HostAwareMemberGroupFactory());
    }

    public static PartitionStateGenerator newConfigPartitionStateGenerator(PartitionGroupConfig partitionGroupConfig) {
        return newCustomPartitionStateGenerator(newMemberGroupFactory(partitionGroupConfig));
    }

    public static PartitionStateGenerator newCustomPartitionStateGenerator(MemberGroupFactory nodeGroupFactory) {
        return new PartitionStateGeneratorImpl(nodeGroupFactory);
    }

    public static MemberGroupFactory newMemberGroupFactory(PartitionGroupConfig partitionGroupConfig) {
        if (partitionGroupConfig == null || !partitionGroupConfig.isEnabled()) {
            return new SingleMemberGroupFactory();
        }
        switch (partitionGroupConfig.getGroupType()) {
            case HOST_AWARE:
                return new HostAwareMemberGroupFactory();
            case CUSTOM:
                return new ConfigMemberGroupFactory(partitionGroupConfig.getMemberGroupConfigs());
            default:
                return new SingleMemberGroupFactory();
        }
    }
}
