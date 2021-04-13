/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.Timers;
import com.hazelcast.jet.impl.execution.init.Contexts.MetaSupplierCtx;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefix;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefixedLogger;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.getJetInstance;
import static com.hazelcast.jet.impl.util.Util.toList;

public final class ExecutionPlanBuilder {

    private ExecutionPlanBuilder() {
    }

    public static Map<MemberInfo, ExecutionPlan> createExecutionPlans(
            NodeEngine nodeEngine, MembersView membersView, DAG dag, long jobId, long executionId,
            JobConfig jobConfig, long lastSnapshotId, boolean isLightJob
    ) {
        Timers.i().execPlanBuilder_createPlans.start();
        final JetInstance instance = getJetInstance(nodeEngine);
        final int defaultParallelism = instance.getConfig().getInstanceConfig().getCooperativeThreadCount();
        final Set<MemberInfo> members = new HashSet<>(membersView.size());
        final Address[] partitionOwners = new Address[nodeEngine.getPartitionService().getPartitionCount()];
        initPartitionOwnersAndMembers(nodeEngine, membersView, members, partitionOwners);

        final List<Address> addresses = toList(members, MemberInfo::getAddress);
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
            // The local parallelism determination here is effective only
            // in jobs submitted as DAG. Otherwise, in jobs submitted as
            // pipeline, we are already doing this determination while
            // converting it to DAG and there is no vertex left as LP=-1.
            final int localParallelism = vertex.determineLocalParallelism(defaultParallelism);
            final int totalParallelism = localParallelism * clusterSize;
            final List<EdgeDef> inbound = toEdgeDefs(dag.getInboundEdges(vertex.getName()), defaultEdgeConfig,
                    e -> vertexIdMap.get(e.getSourceName()), isJobDistributed);
            final List<EdgeDef> outbound = toEdgeDefs(dag.getOutboundEdges(vertex.getName()), defaultEdgeConfig,
                    e -> vertexIdMap.get(e.getDestName()), isJobDistributed);
            String prefix = prefix(jobConfig.getName(), jobId, vertex.getName(), "#PMS");
            ILogger logger = prefixedLogger(nodeEngine.getLogger(metaSupplier.getClass()), prefix);
            try {
                metaSupplier.init(new MetaSupplierCtx(instance, jobId, executionId, jobConfig, logger,
                        vertex.getName(), localParallelism, totalParallelism, clusterSize,
                        jobConfig.getProcessingGuarantee()));
            } catch (Exception e) {
                throw sneakyThrow(e);
            }

            Function<? super Address, ? extends ProcessorSupplier> procSupplierFn = metaSupplier.get(addresses);
            for (Entry<MemberInfo, ExecutionPlan> e : plans.entrySet()) {
                final ProcessorSupplier processorSupplier = procSupplierFn.apply(e.getKey().getAddress());
                if (!isLightJob) {
                    checkSerializable(processorSupplier, "ProcessorSupplier in vertex '" + vertex.getName() + '\'');
                }
                final VertexDef vertexDef = new VertexDef(vertexId, vertex.getName(), processorSupplier, localParallelism);
                vertexDef.addInboundEdges(inbound);
                vertexDef.addOutboundEdges(outbound);
                e.getValue().addVertex(vertexDef);
            }
        }
        Timers.i().execPlanBuilder_createPlans.stop();
        return plans;
    }

    private static Map<String, Integer> assignVertexIds(DAG dag) {
        Map<String, Integer> vertexIdMap = new LinkedHashMap<>();
        final int[] vertexId = {0};
        dag.forEach(v -> vertexIdMap.put(v.getName(), vertexId[0]++));
        return vertexIdMap;
    }

    private static List<EdgeDef> toEdgeDefs(
            List<Edge> edges, EdgeConfig defaultEdgeConfig,
            Function<Edge, Integer> oppositeVtxId, boolean isJobDistributed
    ) {
        List<EdgeDef> list = new ArrayList<>(edges.size());
        for (Edge edge : edges) {
            list.add(new EdgeDef(edge, edge.getConfig() == null ? defaultEdgeConfig : edge.getConfig(),
                    oppositeVtxId.apply(edge), isJobDistributed));
        }
        return list;
    }

    private static void initPartitionOwnersAndMembers(NodeEngine nodeEngine,
                                                      MembersView membersView,
                                                      Set<MemberInfo> members,
                                                      Address[] partitionOwners) {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        Set<Address> addresses = new HashSet<>(membersView.size());
        // TODO [viliam] can we do this faster?
        for (int partitionId = 0; partitionId < partitionOwners.length; partitionId++) {
            Address address = partitionService.getPartitionOwnerOrWait(partitionId);
            partitionOwners[partitionId] = address;
            addresses.add(address);
        }
        for (Address address : addresses) {
            MemberInfo member = membersView.getMember(address);
            if (member == null) {
                // Address in partition table doesn't exist in member list,
                // it has just joined the cluster.
                throw new TopologyChangedException("Topology changed, " + address + " is not in original member list");
            }
            members.add(member);
        }
    }
}
