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

package com.hazelcast.internal.partition;

import com.hazelcast.cluster.Address;

/**
 * Internal event that is dispatched to {@link PartitionAwareService#onPartitionLost}
 * <p>
 * It contains the partition ID, number of replicas that is lost and the address of node that detects the partition lost.
 */
public interface IPartitionLostEvent {

    /**
     * The partition ID that is lost.
     *
     * @return the partition ID that is lost
     */
    int getPartitionId();

    /**
     * 0-based replica index that is lost for the partition.
     * <p>
     * For instance, 0 means only the owner of the partition is lost, 1 means both the owner and first backup are lost.
     *
     * @return 0-based replica index that is lost for the partition
     */
    int getLostReplicaIndex();

    /**
     * The address of the node that detects the partition lost.
     *
     * @return the address of the node that detects the partition lost
     */
    Address getEventSource();

}
