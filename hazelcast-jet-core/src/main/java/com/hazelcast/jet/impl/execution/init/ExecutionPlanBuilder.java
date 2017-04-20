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

import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.TopologyChangedException;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.impl.execution.init.Contexts.MetaSupplierCtx;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.Util.getJetInstance;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class ExecutionPlanBuilder {

    private ExecutionPlanBuilder() {

    }

    public static Map<Member, ExecutionPlan> createExecutionPlans(
            NodeEngine nodeEngine, DAG dag, int defaultParallelism
    ) {
        JetInstance instance = getJetInstance(nodeEngine);
        final Collection<Member> members = new HashSet<>(nodeEngine.getClusterService().getSize());
        final Address[] partitionOwners = new Address[nodeEngine.getPartitionService().getPartitionCount()];
        initPartitionOwnersAndMembers(nodeEngine, members, partitionOwners);

        final List<Address> addresses = members.stream().map(Member::getAddress).collect(toList());
        final int clusterSize = members.size();
        final boolean isJobDistributed = clusterSize > 1;
        final EdgeConfig defaultEdgeConfig = instance.getConfig().getDefaultEdgeConfig();
        final Map<Member, ExecutionPlan> plans =
                members.stream().collect(toMap(m -> m, m -> new ExecutionPlan(partitionOwners)));
        final Map<String, Integer> vertexIdMap = assignVertexIds(dag);
        for (Entry<String, Integer> entry : vertexIdMap.entrySet()) {
            final Vertex vertex = dag.getVertex(entry.getKey());
            final int vertexId = entry.getValue();
            final int localParallelism =
                    vertex.getLocalParallelism() != -1 ? vertex.getLocalParallelism() : defaultParallelism;
            final int totalParallelism = localParallelism * clusterSize;
            final List<EdgeDef> inbound = toEdgeDefs(dag.getInboundEdges(vertex.getName()), defaultEdgeConfig,
                    e -> vertexIdMap.get(e.getSourceName()), isJobDistributed);
            final List<EdgeDef> outbound = toEdgeDefs(dag.getOutboundEdges(vertex.getName()), defaultEdgeConfig,
                    e -> vertexIdMap.get(e.getDestName()), isJobDistributed);
            final ProcessorMetaSupplier metaSupplier = vertex.getSupplier();
            metaSupplier.init(new MetaSupplierCtx(instance, totalParallelism, localParallelism));

            Function<Address, ProcessorSupplier> procSupplierFn = metaSupplier.get(addresses);
            int procIdxOffset = 0;
            for (Entry<Member, ExecutionPlan> e : plans.entrySet()) {
                final ProcessorSupplier processorSupplier = procSupplierFn.apply(e.getKey().getAddress());
                final VertexDef vertexDef = new VertexDef(vertexId, vertex.getName(), processorSupplier,
                        procIdxOffset, localParallelism);
                vertexDef.addInboundEdges(inbound);
                vertexDef.addOutboundEdges(outbound);
                e.getValue().addVertex(vertexDef);
                procIdxOffset += localParallelism;
            }
        }
        return plans;
    }

    private static Map<String, Integer> assignVertexIds(DAG dag) {
        Map<String, Integer> vertexIdMap = new LinkedHashMap<>();
        final int[] vertexId = {0};
        dag.forEach(v -> vertexIdMap.put(v.getName(), vertexId[0]++));
        return vertexIdMap;
    }

    private static List<EdgeDef> toEdgeDefs(List<Edge> edges, EdgeConfig defaultEdgeConfig,
                                            Function<Edge, Integer> oppositeVtxId, boolean isJobDistributed
    ) {
        return edges.stream()
                    .map(edge -> new EdgeDef(edge, edge.getConfig() == null ? defaultEdgeConfig : edge.getConfig(),
                            oppositeVtxId.apply(edge), isJobDistributed))
                    .collect(toList());
    }

    private static void initPartitionOwnersAndMembers(NodeEngine nodeEngine, Collection<Member> members,
                                                      Address[] partitionOwners) {
        ClusterService clusterService = nodeEngine.getClusterService();
        IPartitionService partitionService = nodeEngine.getPartitionService();
        for (int partitionId = 0; partitionId < partitionOwners.length; partitionId++) {
            Address address = partitionService.getPartitionOwnerOrWait(partitionId);

            Member member;
            if ((member = clusterService.getMember(address)) == null) {
                // Address in partition table doesn't exist in member list,
                // it has just left the cluster.
                throw new TopologyChangedException("Topology changed! " + address + " is not member anymore!");
            }

            // add member to known members
            members.add(member);
            partitionOwners[partitionId] = address;
        }
    }
}
