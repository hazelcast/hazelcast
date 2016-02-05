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

package com.hazelcast.jet.api.actor;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.spi.strategy.HashingStrategy;
import com.hazelcast.jet.spi.strategy.ShufflingStrategy;
import com.hazelcast.jet.api.data.io.ProducerInputStream;

/**
 * This is an abstract interface for each consumer in the system
 * which consumes tuple
 */

public interface ObjectConsumer extends Consumer<ProducerInputStream<Object>> {
    /**
     * @param chunk - chunk of Tuples to consume
     * @return really consumed amount of tuples
     * @throws Exception
     */

    int consumeChunk(ProducerInputStream<Object> chunk) throws Exception;

    /**
     * @param object - object to consume
     * @return 1 if tuple was consumed , 0 otherwise
     * @throws Exception
     */
    int consumeObject(Object object) throws Exception;

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
     * @return last consumed tuple count
     */
    int lastConsumedCount();

    /**
     * @return tuple consumer's shuffling strategy
     * null if consumer doesn't support shuffling
     */
    ShufflingStrategy getShufflingStrategy();

    /**
     * Return partitioning strategy
     */
    PartitioningStrategy getPartitionStrategy();

    /**
     * @return hashing strategy
     */
    HashingStrategy getHashingStrategy();
}
