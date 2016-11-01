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

package com.hazelcast.jet2.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.List;

import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;

class RemoteOutboundCollector implements OutboundCollector {

    private final Connection connection;
    private final String engineName;
    private final int executionId;
    private final int destinationVertex;
    private final List<Integer> partitions;
    private final NodeEngineImpl engine;

    public RemoteOutboundCollector(NodeEngine engine,
                                   String engineName,
                                   Address destinationAddress,
                                   int executionId,
                                   int destinationVertex,
                                   List<Integer> partitions) {
        this.engineName = engineName;
        this.executionId = executionId;
        this.destinationVertex = destinationVertex;
        this.engine = (NodeEngineImpl) engine;
        this.connection = this.engine.getNode().getConnectionManager().getConnection(destinationAddress);
        this.partitions = partitions;

    }

    @Override
    public ProgressState offer(Object item) {
        return offer(item, -1);
    }

    @Override
    public ProgressState offer(Object item, int partitionId) {
        byte[] payload = engine.toData(new Payload(engineName, executionId,
                destinationVertex, partitionId, item)).toByteArray();
        connection.write(new Packet(payload).setFlag(Packet.FLAG_JET));
        return ProgressState.DONE;
    }

    @Override
    public ProgressState close() {
        return offer(DONE_ITEM);
    }

    @Override
    public List<Integer> getPartitions() {
        return partitions;
    }

}