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

import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.api.data.CompletionAwareProducer;
import com.hazelcast.jet.spi.strategy.DataTransferringStrategy;

/**
 * This is an abstract interface for each producer in the system
 * which produce data objects
 */
public interface ObjectProducer extends Producer<Object[]>, CompletionAwareProducer {
    /**
     * @return last produced object's count
     */
    int lastProducedCount();

    /**
     * @return true if producer supports shuffling, false otherwise
     */
    boolean isShuffled();

    /**
     * @return corresponding vertex
     */
    Vertex getVertex();

    /**
     * @return producer's name
     */
    String getName();

    /**
     * @return true if producer is closed , false otherwise
     */
    boolean isClosed();

    /**
     * Open current producer
     */
    void open();

    /**
     * Close current producer
     */
    void close();

    /**
     * @return data transferring strategy
     */
    DataTransferringStrategy getDataTransferringStrategy();
}
