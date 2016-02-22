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

package com.hazelcast.jet.spi.dag;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.spi.strategy.DataTransferringStrategy;
import com.hazelcast.jet.spi.strategy.HashingStrategy;
import com.hazelcast.jet.spi.strategy.ProcessingStrategy;
import com.hazelcast.jet.spi.strategy.ShufflingStrategy;

/**
 * Represents abstract edge of DAG;
 */
public interface Edge extends DagElement {
    /**
     * @return - name of the edge;
     */
    String getName();

    /**
     * @return - input vertex of edge;
     */
    Vertex getInputVertex();

    /**
     * @return - output vertex of edge;
     */
    Vertex getOutputVertex();

    /**
     * @return - true if edge will take into account cluster;
     * false - if data through edge's channel will be passed only
     * locally without shuffling;
     */
    boolean isShuffled();

    /**
     * @return - Edge's shuffling strategy;
     */
    ShufflingStrategy getShufflingStrategy();

    /**
     * @return - Edge's processing strategy;
     */
    ProcessingStrategy getProcessingStrategy();

    /**
     * @return - Edge's partitioning strategy;
     */
    PartitioningStrategy getPartitioningStrategy();

    /**
     * @return - Edge's hashing strategy;
     */
    HashingStrategy getHashingStrategy();

    /**
     * @return - Edge's data-transferring strategy;
     */
    DataTransferringStrategy getDataTransferringStrategy();
}
