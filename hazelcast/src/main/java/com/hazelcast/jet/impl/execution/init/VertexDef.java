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

import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.MasterJobContext;
import com.hazelcast.jet.impl.ProcessorClassLoaderTLHolder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.impl.util.ImdgUtil.readList;
import static com.hazelcast.jet.impl.util.ImdgUtil.writeList;
import static com.hazelcast.jet.impl.util.Util.doWithClassLoader;

public class VertexDef implements IdentifiedDataSerializable {

    private int id;
    private List<EdgeDef> inboundEdges = new ArrayList<>();
    private List<EdgeDef> outboundEdges = new ArrayList<>();
    private String name;
    private ProcessorSupplier processorSupplier;
    private int localParallelism;

    VertexDef() {
    }

    VertexDef(int id, String name, ProcessorSupplier processorSupplier, int localParallelism) {
        this.id = id;
        this.name = name;
        this.processorSupplier = processorSupplier;
        this.localParallelism = localParallelism;
    }

    public String name() {
        return name;
    }

    public int localParallelism() {
        return localParallelism;
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

    public ProcessorSupplier processorSupplier() {
        return processorSupplier;
    }

    /**
     * Returns IDs of vertices that meet one of the following criteria:<ul>
     *     <li>it's a higher-priority source for some of its downstream vertices
     *     <li>its output is connected by a snapshot restore edge*
     *     <li>it sits upstream of a vertex meeting the above conditions
     * </ul>
     *
     * (*) We consider all snapshot-restoring vertices to be higher priority
     * because we want to prevent a snapshot before the snapshot restoring is
     * done. If a snapshot-restoring vertex is connected to a source, it would
     * be its only input and therefore will not be higher-priority upstream
     * otherwise.
     * Fixes https://github.com/hazelcast/hazelcast-jet/pull/1101
     */
    static Set<Integer> getHigherPriorityVertices(List<VertexDef> vertices) {
        // We assume the vertices are in topological order. We iterate in the reverse order, this way
        // when we observe a parent, all children should have been observed.
        // That order is asserted in `seenVertices`
        Set<Integer> res = new HashSet<>();
        Set<Integer> seenVertices = new HashSet<>();
        for (int i = vertices.size(); i-- > 0; ) {
            VertexDef v = vertices.get(i);
            assert seenVertices.add(v.vertexId()) : "duplicate vertex id";
            for (EdgeDef outboundEdge : v.outboundEdges) {
                VertexDef downstream = outboundEdge.destVertex();
                assert seenVertices.contains(downstream.vertexId()) : "missing child";
                if (res.contains(downstream.vertexId())
                        || outboundEdge.isSnapshotRestoreEdge()
                        || downstream.inboundEdges.stream()
                                .anyMatch(edge -> edge.priority() > outboundEdge.priority())) {
                    boolean unique = res.add(v.id);
                    assert unique;
                    break;
                }
            }
        }
        return res;
    }

    boolean isSnapshotVertex() {
        return name.startsWith(MasterJobContext.SNAPSHOT_VERTEX_PREFIX);
    }

    @Override
    public String toString() {
        return "VertexDef{name='" + name + '\'' + '}';
    }

    //             IdentifiedDataSerializable implementation

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.VERTEX_DEF;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(id);
        out.writeString(name);
        writeList(out, inboundEdges);
        writeList(out, outboundEdges);
        CustomClassLoadedObject.write(out, processorSupplier);
        out.writeInt(localParallelism);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readInt();
        name = in.readString();
        inboundEdges = readList(in);
        outboundEdges = readList(in);
        processorSupplier = doWithClassLoader(
                ProcessorClassLoaderTLHolder.get(name),
                () -> CustomClassLoadedObject.read(in)
        );
        localParallelism = in.readInt();
    }
}
