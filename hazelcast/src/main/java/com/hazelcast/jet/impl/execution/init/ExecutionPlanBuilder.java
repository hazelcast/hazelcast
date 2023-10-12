/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.collection.IntHashSet;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobClassLoaderService;
import com.hazelcast.jet.impl.execution.init.Contexts.MetaSupplierCtx;
import com.hazelcast.jet.impl.util.FixedCapacityIntArrayList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.config.JobConfigArguments.KEY_REQUIRED_PARTITIONS;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefix;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefixedLogger;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.doWithClassLoader;
import static com.hazelcast.jet.impl.util.Util.range;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.JOB_OFFLOADABLE_EXECUTOR;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.stream.Collectors.toMap;

public final class ExecutionPlanBuilder {

    private ExecutionPlanBuilder() {
    }

    @SuppressWarnings({"checkstyle:ParameterNumber", "rawtypes"})
    public static CompletableFuture<Map<MemberInfo, ExecutionPlan>> createExecutionPlans(
            NodeEngineImpl nodeEngine,
            List<MemberInfo> memberInfos,
            DAG dag,
            long jobId,
            long executionId,
            JobConfig jobConfig,
            long lastSnapshotId,
            boolean isLightJob,
            Subject subject
    ) {
        final Map<MemberInfo, int[]> partitionsByMember;
        final Set<Integer> requiredPartitions = jobConfig.getArgument(KEY_REQUIRED_PARTITIONS);

        if (requiredPartitions != null) {
            PartitionPruningAnalysisResult analysisResult = analyzeDagForPartitionPruning(nodeEngine, dag);
            partitionsByMember = getPartitionAssignment(
                    nodeEngine, memberInfos,
                    analysisResult.allPartitionsRequired,
                    requiredPartitions,
                    analysisResult.constantPartitionIds,
                    analysisResult.requiredAddresses);
        } else {
            partitionsByMember = getPartitionAssignment(
                    nodeEngine, memberInfos,
                    false,
                    null,
                    null,
                    null);
        }

        final Map<Address, int[]> partitionsByAddress = partitionsByMember
                .entrySet()
                .stream()
                .collect(toMap(en -> en.getKey().getAddress(), Entry::getValue));
        final int clusterSize = partitionsByAddress.size();
        final boolean isJobDistributed = clusterSize > 1;

        final VerticesIdAndOrder verticesIdAndOrder = VerticesIdAndOrder.assignVertexIds(dag);
        final int defaultParallelism = nodeEngine.getConfig().getJetConfig().getCooperativeThreadCount();
        final EdgeConfig defaultEdgeConfig = nodeEngine.getConfig().getJetConfig().getDefaultEdgeConfig();

        final Map<MemberInfo, ExecutionPlan> plans = new HashMap<>();
        int memberIndex = 0;
        for (MemberInfo member : partitionsByMember.keySet()) {
            plans.put(member, new ExecutionPlan(partitionsByAddress, jobConfig, lastSnapshotId, memberIndex++,
                    clusterSize, isLightJob, subject, verticesIdAndOrder.count()));
        }

        final List<Address> addresses = toList(partitionsByMember.keySet(), MemberInfo::getAddress);
        ExecutorService initOffloadExecutor = nodeEngine.getExecutionService().getExecutor(JOB_OFFLOADABLE_EXECUTOR);
        CompletableFuture[] futures = new CompletableFuture[verticesIdAndOrder.count()];
        for (VertexIdPos entry : verticesIdAndOrder) {
            final Vertex vertex = dag.getVertex(entry.vertexName);
            assert vertex != null;
            final ProcessorMetaSupplier metaSupplier = vertex.getMetaSupplier();
            final int vertexId = entry.vertexId;
            // The local parallelism determination here is effective only
            // in jobs submitted as DAG. Otherwise, in jobs submitted as
            // pipeline, we are already doing this determination while
            // converting it to DAG and there is no vertex left with LP=-1.
            final int localParallelism = vertex.determineLocalParallelism(defaultParallelism);
            final int totalParallelism = localParallelism * clusterSize;
            final List<EdgeDef> inbound = toEdgeDefs(dag.getInboundEdges(vertex.getName()), defaultEdgeConfig,
                    e -> verticesIdAndOrder.idByName(e.getSourceName()), isJobDistributed);
            final List<EdgeDef> outbound = toEdgeDefs(dag.getOutboundEdges(vertex.getName()), defaultEdgeConfig,
                    e -> verticesIdAndOrder.idByName(e.getDestName()), isJobDistributed);
            String prefix = prefix(jobConfig.getName(), jobId, vertex.getName(), "#PMS");
            ILogger logger = prefixedLogger(nodeEngine.getLogger(metaSupplier.getClass()), prefix);

            RunnableEx action = () -> {
                JetServiceBackend jetBackend = nodeEngine.getService(JetServiceBackend.SERVICE_NAME);
                JobClassLoaderService jobClassLoaderService = jetBackend.getJobClassLoaderService();
                ClassLoader processorClassLoader = jobClassLoaderService.getClassLoader(jobId);
                try {
                    doWithClassLoader(processorClassLoader, () ->
                            metaSupplier.init(new MetaSupplierCtx(nodeEngine, jobId, executionId,
                                    jobConfig, logger, vertex.getName(), localParallelism, totalParallelism, clusterSize,
                                    isLightJob, partitionsByAddress, subject, processorClassLoader)));
                } catch (Exception e) {
                    throw sneakyThrow(peel(e));
                }

                Function<? super Address, ? extends ProcessorSupplier> procSupplierFn =
                        doWithClassLoader(processorClassLoader, () -> metaSupplier.get(addresses));
                for (Entry<MemberInfo, ExecutionPlan> e : plans.entrySet()) {
                    final ProcessorSupplier processorSupplier =
                            doWithClassLoader(processorClassLoader, () -> procSupplierFn.apply(e.getKey().getAddress()));
                    if (!isLightJob) {
                        // We avoid the check for light jobs - the user will get the error anyway, but maybe with less
                        // information. And we can recommend the user to use normal job to have more checks.
                        checkSerializable(processorSupplier, "ProcessorSupplier in vertex '" + vertex.getName() + '\'');
                    }
                    final VertexDef vertexDef = new VertexDef(vertexId, vertex.getName(), processorSupplier, localParallelism);
                    vertexDef.addInboundEdges(inbound);
                    vertexDef.addOutboundEdges(outbound);
                    e.getValue().setVertex(entry.requiredPosition, vertexDef);
                }
            };
            Executor executor = metaSupplier.initIsCooperative() ? CALLER_RUNS : initOffloadExecutor;
            futures[entry.requiredPosition] = runAsync(action, executor);
        }
        return CompletableFuture.allOf(futures)
                .thenCompose(r -> completedFuture(plans));
    }

    /**
     * Analyze DAG if the query uses partition pruning in order to determine
     * which additional members and partitions are necessary for execution
     */
    // visible for testing
    @Nonnull
    static PartitionPruningAnalysisResult analyzeDagForPartitionPruning(NodeEngine nodeEngine, DAG dag) {
        final IPartitionService partitionService = nodeEngine.getPartitionService();
        final int partitionCount = partitionService.getPartitionCount();
        // we expect only local member to be explicitly required
        Set<Address> requiredAddresses = new HashSet<>(1);
        IntHashSet constantPartitionIds = new IntHashSet(partitionCount, -1);
        boolean allPartitionsRequired = false;
        for (Iterator<Edge> it = dag.edgeIterator(); it.hasNext(); ) {
            Edge edge = it.next();
            if (edge.getDistributedTo() != null && !edge.isDistributed()) {
                // Edge is distributed to specific member, not to all members
                // so such member must be included in the job.
                // Usually this will be the local member.
                requiredAddresses.add(edge.getDistributedTo());
            }
            if (edge.getRoutingPolicy() == Edge.RoutingPolicy.PARTITIONED) {
                assert edge.getPartitioner() != null : "PARTITIONED policy was used without partitioner";
                // note that partitioned edge can be either distributed or local.
                var maybeConstantPartition = edge.getPartitioner().getConstantPartitioningKey();
                if (maybeConstantPartition != null) {
                    // allToOne or other constant partitioning case
                    constantPartitionIds.add(partitionService.getPartitionId(maybeConstantPartition));
                } else {
                    // partitioned edge with arbitrary partitioning function.
                    // unable to determine what partition ids will we used.
                    allPartitionsRequired = true;
                }
            }
        }
        // After the analysis we can have both ALL_PARTITIONS_REQUIRED and non-empty constantPartitionIds.
        // This is not a problem, ALL_PARTITIONS_REQUIRED will be more important.
        return new PartitionPruningAnalysisResult(requiredAddresses, constantPartitionIds, allPartitionsRequired);
    }

    // visible for testing
    static class PartitionPruningAnalysisResult {
        final Set<Address> requiredAddresses;
        final Set<Integer> constantPartitionIds;
        final boolean allPartitionsRequired;

        PartitionPruningAnalysisResult(Set<Address> requiredAddresses,
                                       Set<Integer> constantPartitionIds,
                                       boolean allPartitionsRequired) {
            this.requiredAddresses = requiredAddresses;
            this.constantPartitionIds = constantPartitionIds;
            this.allPartitionsRequired = allPartitionsRequired;
        }
    }

    /**
     * Basic vertex data wrapper:
     * - id
     * - name
     * - position
     */
    private static final class VerticesIdAndOrder implements Iterable<VertexIdPos> {
        private final LinkedHashMap<String, Integer> vertexIdMap;
        private final HashMap<Integer, Integer> vertexPosById;

        private VerticesIdAndOrder(LinkedHashMap<String, Integer> vertexIdMap) {
            this.vertexIdMap = vertexIdMap;
            int index = 0;
            vertexPosById = new LinkedHashMap<>(vertexIdMap.size());
            for (Integer vertexId : vertexIdMap.values()) {
                vertexPosById.put(vertexId, index++);
            }
        }

        private Integer idByName(String vertexName) {
            return vertexIdMap.get(vertexName);
        }

        private static VerticesIdAndOrder assignVertexIds(DAG dag) {
            LinkedHashMap<String, Integer> vertexIdMap = new LinkedHashMap<>();
            final int[] vertexId = {0};
            dag.forEach(v -> vertexIdMap.put(v.getName(), vertexId[0]++));
            return new VerticesIdAndOrder(vertexIdMap);
        }

        private int count() {
            return vertexIdMap.size();
        }

        @Nonnull
        @Override
        public Iterator<VertexIdPos> iterator() {
            return vertexIdMap.entrySet().stream()
                    .map(e -> new VertexIdPos(e.getValue(), e.getKey(), vertexPosById.get(e.getValue())))
                    .iterator();
        }
    }

    private static final class VertexIdPos {
        private final int vertexId;
        private final String vertexName;

        /**
         * Position on vertices list that vertex with this id/name should occupy.
         * {@link ExecutionPlan#getVertices()} order matters, it must be the same as DAG iteration order,
         * otherwise some functions in further processing won't give good results.
         */
        private final int requiredPosition;

        private VertexIdPos(int vertexId, String vertexName, int position) {
            this.vertexId = vertexId;
            this.vertexName = vertexName;
            this.requiredPosition = position;
        }
    }

    private static List<EdgeDef> toEdgeDefs(
            List<Edge> edges, EdgeConfig defaultEdgeConfig,
            ToIntFunction<Edge> oppositeVtxId, boolean isJobDistributed
    ) {
        List<EdgeDef> list = new ArrayList<>(edges.size());
        for (Edge edge : edges) {
            list.add(new EdgeDef(edge, edge.getConfig() == null ? defaultEdgeConfig : edge.getConfig(),
                    oppositeVtxId.applyAsInt(edge), isJobDistributed));
        }
        return list;
    }

    /**
     * Assign the partitions to their owners. Partitions whose owner isn't in
     * the {@code memberList}, are assigned to one of the members in a round-robin way.
     * Additional parameters are required if partition pruning is used : (dataPartitions != null).
     * Each mapped partitions id array must be sorted.
     *
     * @param allPartitionsRequired        if true, all partitions must be assigned to all required members
     *                                     were chosen to participate in job execution. It is applicable, if
     *                                     DAG contains at least one partitioned edge with non-constant key.
     * @param dataPartitions               set of all required data partitions must be processed by the job
     * @param routingPartitions            set of transitive partitions must be included to the job (allToOne targets)
     * @param extraRequiredMemberAddresses member addresses are targeted by {@link Edge#distributeTo} in job's DAG.
     */
    @SuppressWarnings("DataFlowIssue")
    public static Map<MemberInfo, int[]> getPartitionAssignment(
            NodeEngine nodeEngine,
            List<MemberInfo> memberList,
            boolean allPartitionsRequired,
            @Nullable Set<Integer> dataPartitions,
            @Nullable Set<Integer> routingPartitions,
            @Nullable Set<Address> extraRequiredMemberAddresses) {

        if (allPartitionsRequired) {
            checkNotNull(dataPartitions);
        }

        IPartitionService partitionService = nodeEngine.getPartitionService();

        Map<Address, MemberInfo> membersByAddress = new HashMap<>();
        for (MemberInfo memberInfo : memberList) {
            membersByAddress.put(memberInfo.getAddress(), memberInfo);
        }

        Map<MemberInfo, FixedCapacityIntArrayList> partitionsForMember = new HashMap<>();
        int partitionCount = partitionService.getPartitionCount();
        int memberIndex = 0;

        // By default, partition pruning won't be applied, and for this code path
        // it is guaranteed to be only partition assignment loop.
        for (int partitionId : dataPartitions == null ? range(0, partitionCount) : dataPartitions) {
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

        if (dataPartitions != null) {
            extraRequiredMemberAddresses = checkNotNull(extraRequiredMemberAddresses);
            routingPartitions = checkNotNull(routingPartitions);

            // Overall algorithm for partition assignment in case of partition pruning is as follows:
            // 1. Find all members that are owners of partitions with data required for the job (`dataPartitions`) - above.
            // 2. Add members that have explicit routing (`Edge.distributeTo`) if not yet added.
            //    Members found after this step are all members that are needed to execute the job ("required members")
            // 3. Assign additional partitions, which do not store data but are needed for other reasons (mainly routing)
            //    to required members.

            // Interactive prunable queries may require coordinator to be present
            // If coordinator still not captured to participate in the job -- do it.
            extraRequiredMemberAddresses.forEach(requiredMemberAddr -> {
                MemberInfo requiredMemberInfo = membersByAddress.get(requiredMemberAddr);
                if (requiredMemberInfo == null) {
                    // Should not happen for local member, may happen if outdated DAG is used
                    // which refers to no longer present member.
                    throw new JetException("Member with address " + requiredMemberAddr + " not present in the cluster");
                }
                partitionsForMember.computeIfAbsent(requiredMemberInfo, (i) -> {
                    nodeEngine.getLogger(ExecutionPlanBuilder.class)
                            .fine("Adding required member " + requiredMemberAddr + " to partition-pruned job members");
                    // Extra members may get some partitions assigned later, especially for ALL_PARTITIONS_REQUIRED
                    return new FixedCapacityIntArrayList(partitionCount);
                });
            });

            // There is a special case of partition/member pruning: when DAG contains distributed-partitioned edge,
            // we still want to apply member pruning, but we must redirect partitioned items to limited cluster subset.
            // To do that, we assign all unassigned (also they are a non-required) partitions to all required members
            // which are already was filtered by main assignment loop above.
            if (allPartitionsRequired || !routingPartitions.isEmpty()) {
                Set<Integer> partitionsToAssign = allPartitionsRequired
                        ? new HashSet<>(range(0, partitionCount))
                        : new HashSet<>(routingPartitions);
                // do not assign duplicates, possible in both above cases
                partitionsToAssign.removeAll(dataPartitions);

                List<MemberInfo> requiredMembers = new ArrayList<>(partitionsForMember.keySet());
                for (int partitionId : partitionsToAssign) {
                    // Assign remaining partitions to one of the required members in round-robin fashion.
                    // they will be only used for internal routing.
                    //
                    // The partition assignment is not balanced here, so not all members have the same number of partitions
                    // especially for ALL_PARTITIONS_REQUIRED. This is not very important when there are only a few partitions
                    // in dataPartitions, but can make some difference if there are many (e.g. half of them).
                    // This is not obvious where extra partitions should be assigned - maybe we should prefer members
                    // that do not store the data for the job because they will be less loaded?
                    var member = requiredMembers.get(memberIndex++ % requiredMembers.size());
                    partitionsForMember.get(member).add(partitionId);
                }
            }
        }

        Map<MemberInfo, int[]> partitionAssignment = new HashMap<>();
        for (Entry<MemberInfo, FixedCapacityIntArrayList> memberWithPartitions : partitionsForMember.entrySet()) {
            int[] p = memberWithPartitions.getValue().asArray();
            if (dataPartitions != null) {
                Arrays.sort(p);
            }
            partitionAssignment.put(memberWithPartitions.getKey(), p);
        }

        return partitionAssignment;
    }

}
