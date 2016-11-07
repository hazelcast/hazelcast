/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.strategy;

import com.hazelcast.core.Member;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.spi.NodeEngine;

import java.util.Set;

import static java.util.Collections.singleton;

/**
 * Distribution strategy based on a given partition key. All data will be sent to
 * the owner of the given partition key.
 */
class SinglePartitionDistributionStrategy implements MemberDistributionStrategy {
    private final Object partitionKey;


    /**
     * Create a DistributionStrategy with the given partition key
     */
    public SinglePartitionDistributionStrategy(Object partitionKey) {
        this.partitionKey = partitionKey;
    }

    @Override
    public Set<Member> getTargetMembers(JobContext jobContext) {
        NodeEngine nodeEngine = jobContext.getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(
                nodeEngine.getSerializationService().toData(
                        partitionKey, StringAndPartitionAwarePartitioningStrategy.INSTANCE));
        Address owner = nodeEngine.getPartitionService().getPartitionOwner(partitionId);
        return singleton(nodeEngine.getClusterService().getMember(owner));
    }
}
