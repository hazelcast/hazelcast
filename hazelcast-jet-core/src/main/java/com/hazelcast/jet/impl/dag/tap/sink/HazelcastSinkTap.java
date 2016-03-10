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

package com.hazelcast.jet.impl.dag.tap.sink;

import com.hazelcast.jet.impl.actor.shuffling.ShufflingWriter;
import com.hazelcast.jet.impl.dag.tap.source.HazelcastListPartitionReader;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.dag.tap.SinkOutputStream;
import com.hazelcast.jet.spi.dag.tap.SinkTap;
import com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.spi.dag.tap.TapType;
import com.hazelcast.jet.spi.data.DataWriter;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.NodeEngine;

import java.util.ArrayList;
import java.util.List;


public class HazelcastSinkTap extends SinkTap {
    private static final SinkTapWriteStrategy DEFAULT_TAP_STRATEGY = SinkTapWriteStrategy.CLEAR_AND_REPLACE;

    private final String name;
    private final TapType tapType;
    private final SinkOutputStream sinkOutputStream;
    private final SinkTapWriteStrategy sinkTapWriteStrategy;

    public HazelcastSinkTap(String name, TapType tapType) {
        this(name, tapType, DEFAULT_TAP_STRATEGY);
    }

    public HazelcastSinkTap(String name, TapType tapType, SinkTapWriteStrategy sinkTapWriteStrategy) {
        this.name = name;
        this.tapType = tapType;
        this.sinkTapWriteStrategy = sinkTapWriteStrategy;

        if (TapType.FILE == tapType) {
            this.sinkOutputStream = new FileOutputStream(this.name, this);
        } else {
            this.sinkOutputStream = null;
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TapType getType() {
        return this.tapType;
    }

    @Override
    public DataWriter[] getWriters(NodeEngine nodeEngine, ContainerDescriptor containerDescriptor) {
        List<DataWriter> writers = new ArrayList<DataWriter>();

        if (TapType.HAZELCAST_LIST == tapType) {
            int partitionId = HazelcastListPartitionReader.getPartitionId(nodeEngine, this.name);
            IPartition partition = nodeEngine.getPartitionService().getPartition(partitionId);
            int realPartitionId = ((partition != null) && (partition.isLocal())) ? partitionId : -1;
            writers.add(
                    new ShufflingWriter(
                            HazelcastWriterFactory.getWriter(
                                    this.tapType,
                                    name,
                                    getTapStrategy(),
                                    containerDescriptor,
                                    realPartitionId,
                                    this),
                            nodeEngine,
                            containerDescriptor
                    )
            );
        } else if (TapType.FILE == tapType) {
            writers.add(
                    HazelcastWriterFactory.getWriter(
                            this.tapType,
                            this.name,
                            getTapStrategy(),
                            containerDescriptor,
                            0,
                            this
                    )
            );
        } else {
            for (IPartition partition : nodeEngine.getPartitionService().getPartitions()) {
                if (partition.isLocal()) {
                    writers.add(
                            new ShufflingWriter(
                                    HazelcastWriterFactory.getWriter(
                                            this.tapType,
                                            this.name,
                                            getTapStrategy(),
                                            containerDescriptor,
                                            partition.getPartitionId(),
                                            this
                                    ),
                                    nodeEngine,
                                    containerDescriptor
                            )
                    );
                }
            }
        }

        return writers.toArray(new DataWriter[writers.size()]);
    }

    @Override
    public SinkTapWriteStrategy getTapStrategy() {
        return sinkTapWriteStrategy;
    }

    @Override
    public SinkOutputStream getSinkOutputStream() {
        if (this.sinkOutputStream == null) {
            throw new IllegalStateException(
                    "SinkOutputStream not implemented for sink tap with name="
                            + name
                            + " tapType="
                            + tapType
            );
        }

        return this.sinkOutputStream;
    }
}
