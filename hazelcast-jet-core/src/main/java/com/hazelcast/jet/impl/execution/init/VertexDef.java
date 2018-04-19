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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.readList;
import static com.hazelcast.jet.impl.util.Util.writeList;

public class VertexDef implements IdentifiedDataSerializable {

    private int id;
    private List<EdgeDef> inboundEdges = new ArrayList<>();
    private List<EdgeDef> outboundEdges = new ArrayList<>();
    private String name;
    private ProcessorSupplier processorSupplier;
    private int procIdxOffset;
    private int localParallelism;
    private int totalParallelism;

    VertexDef() {
    }

    VertexDef(
            int id, String name, ProcessorSupplier processorSupplier, int procIdxOffset,
            int localParallelism, int totalParallelism
    ) {
        this.id = id;
        this.name = name;
        this.processorSupplier = processorSupplier;
        this.procIdxOffset = procIdxOffset;
        this.localParallelism = localParallelism;
        this.totalParallelism = totalParallelism;
    }

    String name() {
        return name;
    }

    int localParallelism() {
        return localParallelism;
    }

    int totalParallelism() {
        return totalParallelism;
    }

    int vertexId() {
        return id;
    }

    void addInboundEdges(List<EdgeDef> edges) {
        this.inboundEdges.addAll(edges);
    }

    void addOutboundEdges(List<EdgeDef> edges) {
        this.outboundEdges.addAll(edges);
    }

    List<EdgeDef> inboundEdges() {
        return inboundEdges;
    }

    List<EdgeDef> outboundEdges() {
        return outboundEdges;
    }

    ProcessorSupplier processorSupplier() {
        return processorSupplier;
    }

    int getProcIdxOffset() {
        return procIdxOffset;
    }

    /**
     * Returns true in any of the following cases:<ul>
     *     <li>this vertex is a higher-priority source for some of its
     *         downstream vertices
     *     <li>it sits upstream of such a vertex
     * </ul>
     */
    boolean isHigherPriorityUpstream() {
        for (EdgeDef outboundEdge : outboundEdges) {
            VertexDef downstream = outboundEdge.destVertex();
            if (downstream.inboundEdges.stream()
                                       .anyMatch(edge -> edge.priority() > outboundEdge.priority())
                    || downstream.isHigherPriorityUpstream()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "VertexDef{" +
                "name='" + name + '\'' +
                '}';
    }


    //             IdentifiedDataSerializable implementation

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.VERTEX_DEF;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
        writeList(out, inboundEdges);
        writeList(out, outboundEdges);
        CustomClassLoadedObject.write(out, processorSupplier);
        out.writeInt(procIdxOffset);
        out.writeInt(localParallelism);
        out.writeInt(totalParallelism);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readInt();
        name = in.readUTF();
        inboundEdges = readList(in);
        outboundEdges = readList(in);
        processorSupplier = CustomClassLoadedObject.read(in);
        procIdxOffset = in.readInt();
        localParallelism = in.readInt();
        totalParallelism = in.readInt();
    }
}
