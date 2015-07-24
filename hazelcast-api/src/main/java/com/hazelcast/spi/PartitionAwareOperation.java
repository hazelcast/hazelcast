/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

/**
 * An interface that can be implemented by an operation to indicate that is should
 * be invoked on a particular partition.
 *
 * This interface only has means for documentation purposes. Because every operation has a {@link Operation#getPartitionId()}
 * method, the system will use that to determine if an Operation is partition-aware. So the system is fine if you create
 * an Operation that doesn't implements PartitionAwareOperation, but returns a partitionId equal or larger than 0 (and therefor is
 * partition-specific). But it is also fine if you do implement this PartitionAwareOperation interface, and return -1 as
 * partition-id (and therefor is not specific to a partition).
 *
 * @author mdogan 12/3/12
 */
public interface PartitionAwareOperation {

    /**
     * Gets the partition id.
     *
     * @return the partition id
     * @see Operation#getPartitionId()
     */
    int getPartitionId();

}
