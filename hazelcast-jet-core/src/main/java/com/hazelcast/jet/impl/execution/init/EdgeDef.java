/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.jet.Edge;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.Partitioner;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Map;

public class EdgeDef implements IdentifiedDataSerializable {

    private int oppositeVertexId;
    private int sourceOrdinal;
    private int destOrdinal;
    private int priority;
    private boolean isBuffered;
    private boolean isDistributed;
    private Edge.ForwardingPattern forwardingPattern;
    private Partitioner partitioner;
    private EdgeConfig config;

    // transient fields populated and used after deserialization
    private transient String id;
    private transient VertexDef sourceVertex;
    private transient VertexDef destVertex;


    EdgeDef() {
    }

    EdgeDef(Edge edge, EdgeConfig config, int oppositeVertexId, boolean isJobDistributed) {
        this.oppositeVertexId = oppositeVertexId;
        this.sourceOrdinal = edge.getSourceOrdinal();
        this.destOrdinal = edge.getDestOrdinal();
        this.priority = edge.getPriority();
        this.isBuffered = edge.isBuffered();
        this.isDistributed = isJobDistributed && edge.isDistributed();
        this.forwardingPattern = edge.getForwardingPattern();
        this.partitioner = edge.getPartitioner();
        this.config = config;
    }

    void initTransientFields(Map<Integer, VertexDef> vMap, VertexDef nearVertex, boolean isOutbound) {
        final VertexDef farVertex = vMap.get(oppositeVertexId);
        this.sourceVertex = isOutbound ? nearVertex : farVertex;
        this.destVertex = isOutbound ? farVertex : nearVertex;
        this.id = sourceVertex.vertexId() + ":" + destVertex.vertexId();
    }

    public Edge.ForwardingPattern forwardingPattern() {
        return forwardingPattern;
    }

    public Partitioner partitioner() {
        return partitioner;
    }

    String edgeId() {
        return id;
    }

    VertexDef sourceVertex() {
        return sourceVertex;
    }

    int sourceOrdinal() {
        return sourceOrdinal;
    }

    VertexDef destVertex() {
        return destVertex;
    }

    int destOrdinal() {
        return destOrdinal;
    }

    int priority() {
        return priority;
    }

    boolean isBuffered() {
        return isBuffered;
    }

    boolean isDistributed() {
        return isDistributed;
    }

    EdgeConfig getConfig() {
        return config;
    }


    // IdentifiedDataSerializable implementation

    @Override
    public int getFactoryId() {
        return JetImplDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetImplDataSerializerHook.EDGE_DEF;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(oppositeVertexId);
        out.writeInt(destOrdinal);
        out.writeInt(sourceOrdinal);
        out.writeInt(priority);
        out.writeBoolean(isBuffered);
        out.writeBoolean(isDistributed);
        out.writeObject(forwardingPattern);
        CustomClassLoadedObject.write(out, partitioner);
        out.writeObject(config);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        oppositeVertexId = in.readInt();
        destOrdinal = in.readInt();
        sourceOrdinal = in.readInt();
        priority = in.readInt();
        isBuffered = in.readBoolean();
        isDistributed = in.readBoolean();
        forwardingPattern = in.readObject();
        partitioner = CustomClassLoadedObject.read(in);
        config = in.readObject();
    }
}
