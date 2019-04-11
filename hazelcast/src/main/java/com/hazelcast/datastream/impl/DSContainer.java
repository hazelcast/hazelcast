/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl;

import com.hazelcast.config.DataStreamConfig;
import com.hazelcast.internal.codeneneration.Compiler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class DSContainer {

    private final DSPartition[] partitions;
    private final DataStreamConfig config;
    private final NodeEngineImpl nodeEngine;
    private final Compiler compiler;
    private final DSService service;

    public DSContainer(DataStreamConfig config,
                       DSService service,
                       NodeEngineImpl nodeEngine,
                       Compiler compiler) {
        this.config = config;
        this.service = service;
        this.nodeEngine = nodeEngine;
        this.partitions = new DSPartition[nodeEngine.getPartitionService().getPartitionCount()];
        this.compiler = compiler;
    }

    public DSPartition getPartition(int partitionId) {
        DSPartition partition = partitions[partitionId];
        if (partition == null) {
            partition = new DSPartition(
                    service,
                    partitionId,
                    config,
                    (InternalSerializationService) nodeEngine.getSerializationService(),
                    compiler);
            partitions[partitionId] = partition;
        }
        return partition;
    }
}
