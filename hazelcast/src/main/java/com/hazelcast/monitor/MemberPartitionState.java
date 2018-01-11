/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor;

import com.hazelcast.internal.management.JsonSerializable;

import java.util.List;

/**
 * Partition related statistics
 */
public interface MemberPartitionState extends JsonSerializable {

    /**
     * Get list of owned partitions of the member
     *
     * @return list of owned partitions of the member
     */
    List<Integer> getPartitions();

    /**
     * Returns whether member is safe for shutdown.
     *
     * @return true if member is safe for shutdown, false otherwise
     */
    boolean isMemberStateSafe();

    /**
     * Returns Migration queue size (This statistic is valid only for master)
     *
     * @return Migration queue size (This statistic is valid only for master)
     */
    long getMigrationQueueSize();

}
