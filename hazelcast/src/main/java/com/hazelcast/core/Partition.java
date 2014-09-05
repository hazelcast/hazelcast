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

package com.hazelcast.core;

/**
 * Virtual partition instance.
 * Each partition belongs to a member and this ownership may change when a member joins to or leaves the cluster.
 */
public interface Partition {

    /**
     * Returns id of the partition.
     *
     * @return id of the partition
     */
    int getPartitionId();

    /**
     * Returns owner member of the partition. It can be that null is returned if the owner of a partition has not
     * been established.
     *
     * @return owner member of the partition
     */
    Member getOwner();
}
