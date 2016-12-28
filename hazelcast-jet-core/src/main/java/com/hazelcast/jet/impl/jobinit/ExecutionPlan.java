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

package com.hazelcast.jet.impl.jobinit;

import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartitionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.jet.impl.util.Util.readList;
import static com.hazelcast.jet.impl.util.Util.writeList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ExecutionPlan implements IdentifiedDataSerializable {
    private List<VertexDef> vertices = new ArrayList<>();

    public ExecutionPlan() {
    }

    public List<VertexDef> getVertices() {
        return vertices;
    }

    public void addVertex(VertexDef vertex) {
        vertices.add(vertex);
    }

    @Override
    public int getFactoryId() {
        return JetImplDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetImplDataSerializerHook.EXECUTION_PLAN;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeList(out, vertices);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        vertices = readList(in);
    }

    /**
     * Initializes partitioners on edges and processor suppliers on vertices. Populates
     * the transient fields on edges.
     *
     * @return all processor suppliers found in the plan
     */
    public List<ProcessorSupplier> init(NodeEngine nodeEngine) {
        final Map<Integer, VertexDef> vMap = getVertices().stream().collect(toMap(VertexDef::vertexId, v -> v));
        getVertices().stream().forEach(v -> {
            v.inputs().forEach(e -> e.initTransientFields(vMap, v, false));
            v.outputs().forEach(e -> e.initTransientFields(vMap, v, true));
        });
        final IPartitionService partitionService = nodeEngine.getPartitionService();
        getVertices().stream()
                     .map(VertexDef::outputs)
                     .flatMap(List::stream)
                     .map(EdgeDef::partitioner)
                     .filter(Objects::nonNull)
                     .forEach(p -> p.init(partitionService::getPartitionId));
        return getVertices()
                .stream()
                .peek(v -> {
                    final ProcessorSupplier sup = v.processorSupplier();
                    final int parallelism = v.parallelism();
                    sup.init(new ProcSupplierContext(nodeEngine.getHazelcastInstance(), parallelism));
                })
                .map(VertexDef::processorSupplier)
                .collect(toList());
    }
}

