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

package com.hazelcast.partition.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.membergroup.MemberGroup;

import java.util.Collection;

public interface PartitionStateGenerator {

    /**
     * Returns the initial layout for the partitions.
     * <p/>
     * A 2 dimensional array of addresses is returned where the first index is the partition id, and
     * the second index is the replica index.
     *
     * @param groups
     * @param partitionCount the number of partitions.
     * @return
     */
    Address[][] initialize(final Collection<MemberGroup> groups, final int partitionCount);

    /**
     * Rearranges the partition layout.
     * <p/>
     * This method does not actually change the partitions, but send back the updated layout.
     * <p/>
     * A 2 dimensional array of addresses is returned where the first index is the partition id, and
     * the second index is the replica index.
     *
     * @param groups
     * @param currentState the current partition state.
     * @return
     */
    Address[][] reArrange(final Collection<MemberGroup> groups, final InternalPartition[] currentState);
}
