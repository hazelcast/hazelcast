/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Edge.RoutingPolicy;
import com.hazelcast.jet.core.Partitioner;
import com.hazelcast.jet.impl.MasterJobContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Map;

@SuppressWarnings("checkstyle:declarationorder")
public class EdgeDef implements IdentifiedDataSerializable {

    private int oppositeVertexId;
    private int sourceOrdinal;
    private int destOrdinal;
    private int priority;
    private RoutingPolicy routingPolicy;
    private Partitioner<?> partitioner;
    private EdgeConfig config;
    private ComparatorEx<?> comparator;
    protected Address distributedTo;

    // transient fields populated and used after deserialization
    // non-private for tests
    protected transient String id;
    protected transient VertexDef sourceVertex;
    protected transient VertexDef destVertex;

    EdgeDef() {
    }

    EdgeDef(Edge edge, EdgeConfig config, int oppositeVertexId, boolean isJobDistributed) {
        this.oppositeVertexId = oppositeVertexId;
        this.sourceOrdinal = edge.getSourceOrdinal();
        this.destOrdinal = edge.getDestOrdinal();
        this.priority = edge.getPriority();
        this.distributedTo = isJobDistributed ? edge.getDistributedTo() : null;
        this.routingPolicy = edge.getRoutingPolicy();
        this.partitioner = edge.getPartitioner();
        this.config = config;
        this.comparator = edge.getOrderComparator();
    }

    void initTransientFields(Map<Integer, VertexDef> vMap, VertexDef nearVertex, boolean isOutbound) {
        final VertexDef farVertex = vMap.get(oppositeVertexId);
        this.sourceVertex = isOutbound ? nearVertex : farVertex;
        this.destVertex = isOutbound ? farVertex : nearVertex;
        this.id = sourceVertex.vertexId() + "-" + sourceOrdinal + ':' + destVertex.vertexId() + '-' + destOrdinal;
    }

    public RoutingPolicy routingPolicy() {
        return routingPolicy;
    }

    public Partitioner<?> partitioner() {
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

    boolean isSnapshotRestoreEdge() {
        return priority == MasterJobContext.SNAPSHOT_RESTORE_EDGE_PRIORITY;
    }

    boolean isLocal() {
        return distributedTo == null;
    }

    /**
     * Note: unlike {@link Edge#getDistributedTo()}, this method always returns
     * null if the DAG is about to be run on a single member. See {@linkplain
     * #EdgeDef(Edge, EdgeConfig, int, boolean) constructor}.
     */
    Address getDistributedTo() {
        return distributedTo;
    }

    EdgeConfig getConfig() {
        return config;
    }

    ComparatorEx<?> getOrderComparator() {
        return comparator;
    }


    // IdentifiedDataSerializable implementation

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.EDGE_DEF;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(oppositeVertexId);
        out.writeInt(destOrdinal);
        out.writeInt(sourceOrdinal);
        out.writeInt(priority);
        out.writeObject(distributedTo);
        out.writeString(routingPolicy == null ? null : routingPolicy.name());
        out.writeObject(config);
        CustomClassLoadedObject.write(out, partitioner);
        CustomClassLoadedObject.write(out, comparator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        oppositeVertexId = in.readInt();
        destOrdinal = in.readInt();
        sourceOrdinal = in.readInt();
        priority = in.readInt();
        distributedTo = in.readObject();
        String routingPolicyName = in.readString();
        routingPolicy = routingPolicyName == null ? null : RoutingPolicy.valueOf(routingPolicyName);
        config = in.readObject();
        partitioner = CustomClassLoadedObject.read(in);
        comparator = CustomClassLoadedObject.read(in);
    }

    @Override
    public String toString() {
        return String.format("%s(%d) -> %s(%d)", sourceVertex == null ? "null" : sourceVertex.name(), sourceOrdinal,
                destVertex == null ? "null" : destVertex.name(), destOrdinal);
    }
}
