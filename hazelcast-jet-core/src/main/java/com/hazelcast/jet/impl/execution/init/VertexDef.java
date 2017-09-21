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

import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.MasterContext;
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
    private int parallelism;

    VertexDef() {
    }

    public VertexDef(int id, String name, ProcessorSupplier processorSupplier, int procIdxOffset, int parallelism) {
        this.id = id;
        this.name = name;
        this.processorSupplier = processorSupplier;
        this.procIdxOffset = procIdxOffset;
        this.parallelism = parallelism;
    }

    public int vertexId() {
        return id;
    }

    public void addInboundEdges(List<EdgeDef> edges) {
        this.inboundEdges.addAll(edges);
    }

    public void addOutboundEdges(List<EdgeDef> edges) {
        this.outboundEdges.addAll(edges);
    }

    public List<EdgeDef> inboundEdges() {
        return inboundEdges;
    }

    public List<EdgeDef> outboundEdges() {
        return outboundEdges;
    }

    public ProcessorSupplier processorSupplier() {
        return processorSupplier;
    }

    public String name() {
        return name;
    }

    public int parallelism() {
        return parallelism;
    }

    public int getProcIdxOffset() {
        return procIdxOffset;
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.VERTEX_DEF;
    }

    /**
     * Returns true, if either:<ul>
     *     <li>this vertex is higher priority source for some of it's
     *     downstream vertices
     *
     *     <li>some of it's downstream vertices is higher priority source
     *     itself (determined by recursion)
     *
     *     <li>this vertex has
     *     {@link MasterContext#SNAPSHOT_RESTORE_EDGE_PRIORITY} priority
     * </ul>
     */
    boolean isHigherPriorityUpstream() {
        for (EdgeDef outboundEdge : outboundEdges) {
            if (outboundEdge.priority() == MasterContext.SNAPSHOT_RESTORE_EDGE_PRIORITY
                    || outboundEdge.destVertex().isHigherPriorityUpstream()
                    || outboundEdge.destVertex().inboundEdges.stream()
                              .anyMatch(edge -> edge.priority() > outboundEdge.priority())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
        writeList(out, inboundEdges);
        writeList(out, outboundEdges);
        CustomClassLoadedObject.write(out, processorSupplier);
        out.writeInt(procIdxOffset);
        out.writeInt(parallelism);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readInt();
        name = in.readUTF();
        inboundEdges = readList(in);
        outboundEdges = readList(in);
        processorSupplier = CustomClassLoadedObject.read(in);
        procIdxOffset = in.readInt();
        parallelism = in.readInt();
    }

    @Override
    public String toString() {
        return "VertexDef{" +
                "name='" + name + '\'' +
                '}';
    }
}
