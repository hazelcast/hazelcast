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
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobClassLoaderService;
import com.hazelcast.jet.impl.execution.init.Contexts.MetaSupplierCtx;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.security.auth.Subject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefix;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefixedLogger;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.doWithClassLoader;
import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.stream.Collectors.toMap;

public final class ExecutionPlanBuilder {

    private ExecutionPlanBuilder() {
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    public static Map<MemberInfo, ExecutionPlan> createExecutionPlans(
            NodeEngineImpl nodeEngine, List<MemberInfo> memberInfos, DAG dag, long jobId, long executionId,
            JobConfig jobConfig, long lastSnapshotId, boolean isLightJob, Subject subject
    ) {
        final int defaultParallelism = nodeEngine.getConfig().getJetConfig().getCooperativeThreadCount();
        final Map<MemberInfo, int[]> partitionsByMember = getPartitionAssignment(nodeEngine, memberInfos);
        final Map<Address, int[]> partitionsByAddress =
                partitionsByMember.entrySet().stream().collect(toMap(en -> en.getKey().getAddress(), Entry::getValue));
        final List<Address> addresses = toList(partitionsByMember.keySet(), MemberInfo::getAddress);
        final int clusterSize = partitionsByMember.size();
        final boolean isJobDistributed = clusterSize > 1;
        final EdgeConfig defaultEdgeConfig = nodeEngine.getConfig().getJetConfig().getDefaultEdgeConfig();
        final Map<MemberInfo, ExecutionPlan> plans = new HashMap<>();
        int memberIndex = 0;
        for (MemberInfo member : partitionsByMember.keySet()) {
            plans.put(member, new ExecutionPlan(partitionsByAddress, jobConfig, lastSnapshotId, memberIndex++,
                    clusterSize, isLightJob, subject));
        }
        final Map<String, Integer> vertexIdMap = assignVertexIds(dag);

        for (Entry<String, Integer> entry : vertexIdMap.entrySet()) {
            final Vertex vertex = dag.getVertex(entry.getKey());
            assert vertex != null;
            final ProcessorMetaSupplier metaSupplier = vertex.getMetaSupplier();
            final int vertexId = entry.getValue();
            // The local parallelism determination here is effective only
            // in jobs submitted as DAG. Otherwise, in jobs submitted as
            // pipeline, we are already doing this determination while
            // converting it to DAG and there is no vertex left with LP=-1.
            final int localParallelism = vertex.determineLocalParallelism(defaultParallelism);
            final int totalParallelism = localParallelism * clusterSize;
            final List<EdgeDef> inbound = toEdgeDefs(dag.getInboundEdges(vertex.getName()), defaultEdgeConfig,
                    e -> vertexIdMap.get(e.getSourceName()), isJobDistributed);
            final List<EdgeDef> outbound = toEdgeDefs(dag.getOutboundEdges(vertex.getName()), defaultEdgeConfig,
                    e -> vertexIdMap.get(e.getDestName()), isJobDistributed);
            String prefix = prefix(jobConfig.getName(), jobId, vertex.getName(), "#PMS");
            ILogger logger = prefixedLogger(nodeEngine.getLogger(metaSupplier.getClass()), prefix);

            JetServiceBackend jetBackend = nodeEngine.getService(JetServiceBackend.SERVICE_NAME);
            JobClassLoaderService jobClassLoaderService = jetBackend.getJobClassLoaderService();
            ClassLoader processorClassLoader = jobClassLoaderService.getClassLoader(jobId);
            try {
                doWithClassLoader(processorClassLoader, () ->
                        metaSupplier.init(new MetaSupplierCtx(nodeEngine, jobId, executionId,
                                jobConfig, logger, vertex.getName(), localParallelism, totalParallelism, clusterSize,
                                isLightJob, partitionsByAddress, subject, processorClassLoader)));
            } catch (Exception e) {
                throw sneakyThrow(e);
            }

            Function<? super Address, ? extends ProcessorSupplier> procSupplierFn =
                    doWithClassLoader(processorClassLoader, () -> metaSupplier.get(addresses));
            for (Entry<MemberInfo, ExecutionPlan> e : plans.entrySet()) {
                final ProcessorSupplier processorSupplier =
                        doWithClassLoader(processorClassLoader, () -> procSupplierFn.apply(e.getKey().getAddress()));
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

    /**
     * Assign the partitions to their owners. Partitions whose owner isn't in
     * the {@code memberList}, are assigned to one of the members in a
     * round-robin way.
     */
    public static Map<MemberInfo, int[]> getPartitionAssignment(NodeEngine nodeEngine, List<MemberInfo> memberList) {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        Map<Address, MemberInfo> membersByAddress = new HashMap<>();
        for (MemberInfo memberInfo : memberList) {
            membersByAddress.put(memberInfo.getAddress(), memberInfo);
        }

        Map<MemberInfo, FixedCapacityIntArrayList> partitionsForMember = new HashMap<>();
        int partitionCount = partitionService.getPartitionCount();
        int memberIndex = 0;

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            Address address = partitionService.getPartitionOwnerOrWait(partitionId);
            MemberInfo member = membersByAddress.get(address);
            if (member == null) {
                // if the partition owner isn't in the current memberList, assign to one of the other members in
                // round-robin fashion
                member = memberList.get(memberIndex++ % memberList.size());
            }
            partitionsForMember.computeIfAbsent(member, ignored -> new FixedCapacityIntArrayList(partitionCount))
                    .add(partitionId);
        }

        Map<MemberInfo, int[]> partitionAssignment = new HashMap<>();
        for (Entry<MemberInfo, FixedCapacityIntArrayList> memberWithPartitions : partitionsForMember.entrySet()) {
            partitionAssignment.put(memberWithPartitions.getKey(), memberWithPartitions.getValue().asArray());
        }
        return partitionAssignment;
    }

    static class FixedCapacityIntArrayList {
        private int[] elements;
        private int size;

        FixedCapacityIntArrayList(int capacity) {
            elements = new int[capacity];
        }

        void add(int element) {
            elements[size++] = element;
        }

        int[] asArray() {
            int[] result = size == elements.length ? elements : Arrays.copyOfRange(elements, 0, size);
            elements = null;
            return result;
        }
    }
}
