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

package com.hazelcast.dataseries.impl;

import com.hazelcast.config.DataSeriesConfig;
import com.hazelcast.internal.codeneneration.Compiler;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class DataSeriesContainer {

    private final Partition[] partitions;
    private final DataSeriesConfig config;
    private final NodeEngineImpl nodeEngine;
    private final Compiler compiler;

    public DataSeriesContainer(DataSeriesConfig config, NodeEngineImpl nodeEngine, Compiler compiler) {
        this.config = config;
        this.nodeEngine = nodeEngine;
        this.partitions = new Partition[nodeEngine.getPartitionService().getPartitionCount()];
        this.compiler = compiler;
    }

    public Partition getPartition(int partitionId) {
        Partition partition = partitions[partitionId];
        if (partition == null) {
            partition = new Partition(config, nodeEngine.getSerializationService(), compiler);
            partitions[partitionId] = partition;
        }
        return partition;
    }
}
