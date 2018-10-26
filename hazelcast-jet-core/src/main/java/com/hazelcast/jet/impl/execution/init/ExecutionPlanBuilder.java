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

import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.execution.init.Contexts.MetaSupplierCtx;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.getJetInstance;
import static java.lang.Integer.min;
import static java.util.stream.Collectors.toList;

public final class ExecutionPlanBuilder {

    private ExecutionPlanBuilder() {
    }

    public static Map<MemberInfo, ExecutionPlan> createExecutionPlans(
            NodeEngine nodeEngine, MembersView membersView, DAG dag, long jobId, long executionId,
            JobConfig jobConfig, long lastSnapshotId
    ) {
        final JetInstance instance = getJetInstance(nodeEngine);
        final int defaultParallelism = instance.getConfig().getInstanceConfig().getCooperativeThreadCount();
        final Collection<MemberInfo> members = new HashSet<>(membersView.size());
        final Address[] partitionOwners = new Address[nodeEngine.getPartitionService().getPartitionCount()];
        initPartitionOwnersAndMembers(nodeEngine, membersView, members, partitionOwners);

        final List<Address> addresses = members.stream().map(MemberInfo::getAddress).collect(toList());
        final int clusterSize = members.size();
        final boolean isJobDistributed = clusterSize > 1;
        final EdgeConfig defaultEdgeConfig = instance.getConfig().getDefaultEdgeConfig();
        final Map<MemberInfo, ExecutionPlan> plans = new HashMap<>();
        int memberIndex = 0;
        for (MemberInfo member : members) {
            plans.put(member, new ExecutionPlan(partitionOwners, jobConfig, lastSnapshotId, memberIndex++, clusterSize));
        }
        final Map<String, Integer> vertexIdMap = assignVertexIds(dag);
        for (Entry<String, Integer> entry : vertexIdMap.entrySet()) {
            final Vertex vertex = dag.getVertex(entry.getKey());
            final ProcessorMetaSupplier metaSupplier = vertex.getMetaSupplier();
            final int vertexId = entry.getValue();
            final int localParallelism = determineParallelism(vertex,
                    metaSupplier.preferredLocalParallelism(), defaultParallelism);
            final int totalParallelism = localParallelism * clusterSize;
            final List<EdgeDef> inbound = toEdgeDefs(dag.getInboundEdges(vertex.getName()), defaultEdgeConfig,
                    e -> vertexIdMap.get(e.getSourceName()), isJobDistributed);
            final List<EdgeDef> outbound = toEdgeDefs(dag.getOutboundEdges(vertex.getName()), defaultEdgeConfig,
                    e -> vertexIdMap.get(e.getDestName()), isJobDistributed);
            final ILogger logger = nodeEngine.getLogger(String.format("%s.%s#ProcessorMetaSupplier",
                    metaSupplier.getClass().getName(), vertex.getName()));
            try {
                metaSupplier.init(new MetaSupplierCtx(instance, jobId, executionId, jobConfig, logger,
                        vertex.getName(), localParallelism, totalParallelism, clusterSize));
            } catch (Exception e) {
                throw sneakyThrow(e);
            }

            Function<? super Address, ? extends ProcessorSupplier> procSupplierFn = metaSupplier.get(addresses);
            for (Entry<MemberInfo, ExecutionPlan> e : plans.entrySet()) {
                final ProcessorSupplier processorSupplier = procSupplierFn.apply(e.getKey().getAddress());
                checkSerializable(processorSupplier, "ProcessorSupplier in vertex '" + vertex.getName() + '\'');
                final VertexDef vertexDef = new VertexDef(vertexId, vertex.getName(), processorSupplier, localParallelism);
                vertexDef.addInboundEdges(inbound);
                vertexDef.addOutboundEdges(outbound);
                e.getValue().addVertex(vertexDef);
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

    private static int determineParallelism(Vertex vertex, int preferredLocalParallelism, int defaultParallelism) {
        int localParallelism = vertex.getLocalParallelism();
        Vertex.checkLocalParallelism(preferredLocalParallelism);
        Vertex.checkLocalParallelism(localParallelism);
        return localParallelism != LOCAL_PARALLELISM_USE_DEFAULT
                        ? localParallelism
             : preferredLocalParallelism != LOCAL_PARALLELISM_USE_DEFAULT
                        ? min(preferredLocalParallelism, defaultParallelism)
             : defaultParallelism;
    }

    private static List<EdgeDef> toEdgeDefs(
            List<Edge> edges, EdgeConfig defaultEdgeConfig,
            Function<Edge, Integer> oppositeVtxId, boolean isJobDistributed
    ) {
        return edges.stream()
                    .map(edge -> new EdgeDef(edge, edge.getConfig() == null ? defaultEdgeConfig : edge.getConfig(),
                            oppositeVtxId.apply(edge), isJobDistributed))
                    .collect(toList());
    }

    private static void initPartitionOwnersAndMembers(NodeEngine nodeEngine,
                                                      MembersView membersView,
                                                      Collection<MemberInfo> members,
                                                      Address[] partitionOwners) {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        for (int partitionId = 0; partitionId < partitionOwners.length; partitionId++) {
            Address address = partitionService.getPartitionOwnerOrWait(partitionId);

            MemberInfo member;
            if ((member = membersView.getMember(address)) == null) {
                // Address in partition table doesn't exist in member list,
                // it has just joined the cluster.
                throw new TopologyChangedException("Topology changed, " + address + " is not in original member list");
            }

            // add member to known members
            members.add(member);
            partitionOwners[partitionId] = address;
        }
    }
}
