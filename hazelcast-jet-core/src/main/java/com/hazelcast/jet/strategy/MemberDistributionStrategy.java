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
import com.hazelcast.jet.container.ContainerContext;

import java.io.Serializable;
import java.util.Collection;

/**
 * Strategy which determines which nodes the data should be distributed to.
 * The {@link RoutingStrategy} on the edge will determine which consumers
 * will receive the data on the target node.
 */
public interface MemberDistributionStrategy extends Serializable {
    /**
     * Returns the set of members which the data should be distributed to. The data will be distributed
     * to all the members in the given list and then routed according to the {@link RoutingStrategy}
     * of the edge.
     */
    Collection<Member> getTargetMembers(ContainerContext containerContext);
}
