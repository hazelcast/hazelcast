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

package com.hazelcast.jet.runtime;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.jet.strategy.MemberDistributionStrategy;

/**
 * This is an abstract interface for each consumer in the system
 * which consumes
 */
public interface Consumer {
    /**
     * @return true if write is partition-aware, else otherwise
     * <p/>
     * Examples of partition-aware writers:
     * <pre>
     *          - Map
     *          - Set
     *          - List
     *      </pre>
     */
    default boolean isPartitioned() {
        return true;
    }

    /**
     * @return writer's partition id
     */
    default int getPartitionId() {
        return -1;
    }

    /**
     * Consumes a chunk of objects
     *
     * @return actual consumed number of objects
     */
    int consume(InputChunk<Object> chunk);

    /**
     * Consume given object
     *
     * @return true if object was consumed, false otherwise
     */
    boolean consume(Object object);

    /**
     * @return true if consumer supports shuffling, false otherwise
     */
    boolean isShuffled();

    /**
     * Flush last consumed chunk
     */
    void flush();

    /**
     * @return true if last data has been flushed, false otherwise
     */
    boolean isFlushed();

    /**
     * Opens current consumer
     */
    void open();

    /**
     * Closes current consumer
     */
    void close();

    /**
     * @return pair consumer's shuffling strategy
     * null if consumer doesn't support shuffling
     */
    default MemberDistributionStrategy getMemberDistributionStrategy() {
        return null;
    }

    /**
     * @return the partitioning strategy
     */
    PartitioningStrategy getPartitionStrategy();

    /**
     * @return hashing strategy
     */
    HashingStrategy getHashingStrategy();

}
