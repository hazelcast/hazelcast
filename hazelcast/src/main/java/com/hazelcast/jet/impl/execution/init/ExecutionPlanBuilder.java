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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.execution.init.Contexts.MetaSupplierCtx;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.LightMasterContext.LIGHT_JOB_CONFIG;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefix;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefixedLogger;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public final class ExecutionPlanBuilder {

    private ExecutionPlanBuilder() {
    }

    public static Map<MemberInfo, ExecutionPlan> createExecutionPlans(
            NodeEngine nodeEngine, Collection<MemberInfo> memberInfos, DAG dag, long jobId, long executionId,
            JobConfig jobConfig, long lastSnapshotId, boolean isLightJob
    ) {
        assert !isLightJob || jobConfig == LIGHT_JOB_CONFIG;
        final HazelcastInstance hazelcastInstance = nodeEngine.getHazelcastInstance();
        final int defaultParallelism = hazelcastInstance.getConfig().getJetConfig()
                .getInstanceConfig().getCooperativeThreadCount();
        final Map<Address, int[]> partitionAssignment = getPartitionAssignment(nodeEngine);
        final Set<MemberInfo> members = new HashSet<>(memberInfos.size());
        Map<Address, MemberInfo> membersMap =
                memberInfos.stream().collect(toMap(MemberInfo::getAddress, identity()));
        for (Address address : partitionAssignment.keySet()) {
            MemberInfo member = membersMap.get(address);
            if (member == null) {
                // Address in partition table doesn't exist in member list,
                // it has just joined the cluster.
                throw new TopologyChangedException("Topology changed, " + address + " is not in original member list");
            }
            members.add(member);
        }

        final List<Address> addresses = toList(members, MemberInfo::getAddress);
        final int clusterSize = members.size();
        final boolean isJobDistributed = clusterSize > 1;
        final EdgeConfig defaultEdgeConfig = hazelcastInstance.getConfig().getJetConfig().getDefaultEdgeConfig();
        final Map<MemberInfo, ExecutionPlan> plans = new HashMap<>();
        int memberIndex = 0;
        for (MemberInfo member : members) {
            plans.put(member,
                    new ExecutionPlan(partitionAssignment, jobConfig, lastSnapshotId, memberIndex++, clusterSize, isLightJob));
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
                metaSupplier.init(new MetaSupplierCtx(hazelcastInstance, jobId, executionId, jobConfig, logger,
                        vertex.getName(), localParallelism, totalParallelism, clusterSize, isLightJob, partitionAssignment));
            } catch (Exception e) {
                throw sneakyThrow(e);
            }

            Function<? super Address, ? extends ProcessorSupplier> procSupplierFn = metaSupplier.get(addresses);
            for (Entry<MemberInfo, ExecutionPlan> e : plans.entrySet()) {
                final ProcessorSupplier processorSupplier = procSupplierFn.apply(e.getKey().getAddress());
                if (!isLightJob) {
                    // We avoid the check for light jobs - the user will get the error anyway, but maybe with less information.
                    // And we can recommend the user to use normal job to have more checks.
                    checkSerializable(processorSupplier, "ProcessorSupplier in vertex '" + vertex.getName() + '\'');
                }
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

    public static Map<Address, int[]> getPartitionAssignment(NodeEngine nodeEngine) {
        final Address[] partitionOwners = new Address[nodeEngine.getPartitionService().getPartitionCount()];
        initPartitionOwnersAndMembers(nodeEngine, partitionOwners);
        return IntStream.range(0, partitionOwners.length)
                .mapToObj(i -> tuple2(partitionOwners[i], i))
                .collect(Collectors.groupingBy(Tuple2::f0,
                        Collectors.mapping(Tuple2::f1,
                                Collectors.collectingAndThen(Collectors.<Integer>toList(),
                                        intList -> intList.stream().mapToInt(Integer::intValue).toArray()))));
    }

    private static void initPartitionOwnersAndMembers(NodeEngine nodeEngine,
                                                      Address[] partitionOwners) {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        for (int partitionId = 0; partitionId < partitionOwners.length; partitionId++) {
            Address address = partitionService.getPartitionOwnerOrWait(partitionId);
            partitionOwners[partitionId] = address;
        }
    }
}
