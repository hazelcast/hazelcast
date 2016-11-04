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

import java.io.Serializable;
import java.util.Set;

/**
 * Strategy which determines which nodes the data should be distributed to.
 * The {@link RoutingStrategy} on the edge will determine which consumers
 * will receive the data on the target node.
 */
@FunctionalInterface
public interface MemberDistributionStrategy extends Serializable {
    /**
     * Returns the set of members which the data should be distributed to. The data will be distributed
     * to all the members in the given list and then routed according to the {@link RoutingStrategy}
     * of the edge.
     *
     * @param jobContext the job context
     */
    Set<Member> getTargetMembers(JobContext jobContext);

    /**
     * A strategy that broadcasts emitted objects to all members
     */
    static MemberDistributionStrategy allMembers() {
        return (context) -> context.getNodeEngine().getClusterService().getMembers();
    }

    /**
     * A distribution strategy which shuffles all data to a single node, identified by the given address
     */
    static MemberDistributionStrategy singleMember(Member member) {
        return new SingleMemberDistributionStrategy(member);
    }

    /**
     * Distribution strategy based on a given partition key. All data will be sent to
     * the owner of the given partition key.
     */
    static MemberDistributionStrategy singlePartition(String partitionKey) {
        return new SinglePartitionDistributionStrategy(partitionKey);
    }
}
