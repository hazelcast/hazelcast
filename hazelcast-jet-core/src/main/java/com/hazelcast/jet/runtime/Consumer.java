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
     * @param chunk chunk of objects to consume
     * @return really consumed amount of pairs
     */
    int consume(InputChunk<Object> chunk);

    /**
     * @param object object to consume
     * @return 1 if pair was consumed , 0 otherwise
     */
    int consume(Object object);

    /**
     * @return true if consumer supports shuffling, false otherwise
     */
    boolean isShuffled();

    /**
     * Perfoms flush of last consumed data
     *
     * @return amount of flushed data
     */
    int flush();

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
     * @return last consumed pair count
     */
    int lastConsumedCount();

    /**
     * @return pair consumer's shuffling strategy
     * null if consumer doesn't support shuffling
     */
    MemberDistributionStrategy getMemberDistributionStrategy();

    /**
     * @return the partitioning strategy
     */
    PartitioningStrategy getPartitionStrategy();

    /**
     * @return hashing strategy
     */
    HashingStrategy getHashingStrategy();

}
