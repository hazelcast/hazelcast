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
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.TopologyChangedException;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.ConcurrentInboundEdgeStream;
import com.hazelcast.jet.impl.execution.ConveyorCollector;
import com.hazelcast.jet.impl.execution.ConveyorCollectorWithPartition;
import com.hazelcast.jet.impl.execution.ConveyorEmitter;
import com.hazelcast.jet.impl.execution.InboundEdgeStream;
import com.hazelcast.jet.impl.execution.InboundEmitter;
import com.hazelcast.jet.impl.execution.OutboundCollector;
import com.hazelcast.jet.impl.execution.OutboundEdgeStream;
import com.hazelcast.jet.impl.execution.ProcessorTasklet;
import com.hazelcast.jet.impl.execution.ReceiverTasklet;
import com.hazelcast.jet.impl.execution.SenderTasklet;
import com.hazelcast.jet.impl.execution.Tasklet;
import com.hazelcast.jet.impl.execution.init.Contexts.MetaSupplierCtx;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartitionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.concurrentConveyor;
import static com.hazelcast.jet.impl.execution.OutboundCollector.compositeCollector;
import static com.hazelcast.jet.impl.util.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.util.Util.getRemoteMembers;
import static com.hazelcast.jet.impl.util.Util.readList;
import static com.hazelcast.jet.impl.util.Util.writeList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ExecutionPlan implements IdentifiedDataSerializable {

    private static final ILogger LOGGER = Logger.getLogger(ExecutionPlan.class);

    private final List<Tasklet> tasklets = new ArrayList<>();
    // vertex id --> ordinal --> receiver tasklet
    private final Map<Integer, Map<Integer, ReceiverTasklet>> receiverMap = new HashMap<>();
    // dest vertex id --> dest ordinal --> dest addr --> sender tasklet
    private final Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap = new HashMap<>();

    private Address[] partitionOwners;
    private List<VertexDef> vertices = new ArrayList<>();
    private final Map<String, ConcurrentConveyor<Object>[]> localConveyorMap = new HashMap<>();
    private final Map<String, Map<Address, ConcurrentConveyor<Object>>> edgeSenderConveyorMap = new HashMap<>();
    private PartitionArrangement ptionArrgmt;

    private NodeEngine nodeEngine;
    private long executionId;


    ExecutionPlan() {
    }

    private ExecutionPlan(Address[] partitionOwners) {
        this.partitionOwners = partitionOwners;
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
        final Map<Member, ExecutionPlan> plans = members.stream().collect(toMap(m -> m, m -> new ExecutionPlan(partitionOwners)));
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
            for (Entry<Member, ExecutionPlan> e : plans.entrySet()) {
                final ProcessorSupplier processorSupplier = procSupplierFn.apply(e.getKey().getAddress());
                final VertexDef vertexDef = new VertexDef(vertexId, vertex.getName(), processorSupplier, localParallelism);
                vertexDef.addInboundEdges(inbound);
                vertexDef.addOutboundEdges(outbound);
                e.getValue().vertices.add(vertexDef);
            }
        }
        return plans;
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

    private static JetInstance getJetInstance(NodeEngine nodeEngine) {
        return nodeEngine.<JetService>getService(JetService.SERVICE_NAME).getJetInstance();
    }

    public void initialize(NodeEngine nodeEngine, long executionId) {
        this.nodeEngine = nodeEngine;
        this.executionId = executionId;
        initProcSuppliers();
        initDag();

        this.ptionArrgmt = new PartitionArrangement(partitionOwners, nodeEngine.getThisAddress());
        JetInstance instance = getJetInstance(nodeEngine);
        for (VertexDef srcVertex : vertices) {
            int processorIdx = -1;
            for (Processor p : createProcessors(srcVertex, srcVertex.parallelism())) {
                processorIdx++;
                // createOutboundEdgeStreams() populates localConveyorMap and edgeSenderConveyorMap.
                // Also populates instance fields: senderMap, receiverMap, tasklets.
                final List<OutboundEdgeStream> outboundStreams = createOutboundEdgeStreams(srcVertex, processorIdx);
                final List<InboundEdgeStream> inboundStreams = createInboundEdgeStreams(srcVertex, processorIdx);
                ILogger logger = nodeEngine.getLogger(
                        srcVertex.name() + '(' + p.getClass().getSimpleName() + ")#" + processorIdx);
                ProcCtx context = new ProcCtx(instance, logger, srcVertex.name(), processorIdx);
                tasklets.add(new ProcessorTasklet(srcVertex.name(), context, p, inboundStreams, outboundStreams));
            }
        }
        tasklets.addAll(receiverMap.values().stream().map(Map::values).flatMap(Collection::stream).collect(toList()));
    }

    public List<ProcessorSupplier> getProcessorSuppliers() {
        return vertices.stream().map(VertexDef::processorSupplier).collect(toList());
    }

    public Map<Integer, Map<Integer, ReceiverTasklet>> getReceiverMap() {
        return receiverMap;
    }

    public Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> getSenderMap() {
        return senderMap;
    }

    public List<Tasklet> getTasklets() {
        return tasklets;
    }


    // Implementation of IdentifiedDataSerializable

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
        out.writeInt(partitionOwners.length);
        for (Address address : partitionOwners) {
            out.writeObject(address);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        vertices = readList(in);
        int len = in.readInt();
        partitionOwners = new Address[len];
        for (int i = 0; i < len; i++) {
            partitionOwners[i] = in.readObject();
        }
    }

    // End implementation of IdentifiedDataSerializable


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

    private void initProcSuppliers() {
        JetService service = nodeEngine.getService(JetService.SERVICE_NAME);
        vertices.forEach(v -> v.processorSupplier().init(
                new ProcSupplierCtx(service.getJetInstance(), v.parallelism())));
    }

    private void initDag() {
        final Map<Integer, VertexDef> vMap = vertices.stream().collect(toMap(VertexDef::vertexId, v -> v));
        vertices.forEach(v -> {
            v.inboundEdges().forEach(e -> e.initTransientFields(vMap, v, false));
            v.outboundEdges().forEach(e -> e.initTransientFields(vMap, v, true));
        });
        final IPartitionService partitionService = nodeEngine.getPartitionService();
        vertices.stream()
                .map(VertexDef::outboundEdges)
                .flatMap(List::stream)
                .map(EdgeDef::partitioner)
                .filter(Objects::nonNull)
                .forEach(p -> p.init(partitionService::getPartitionId));
    }

    private static Collection<? extends Processor> createProcessors(VertexDef vertexDef, int parallelism) {
        final Collection<? extends Processor> processors = vertexDef.processorSupplier().get(parallelism);
        if (processors.size() != parallelism) {
            throw new JetException("ProcessorSupplier failed to return the requested number of processors." +
                    " Requested: " + parallelism + ", returned: " + processors.size());
        }
        return processors;
    }

    /**
     * Populates {@code localConveyorMap}, {@code edgeSenderConveyorMap}.
     * Populates {@link #senderMap} and {@link #tasklets} fields.
     */
    private List<OutboundEdgeStream> createOutboundEdgeStreams(VertexDef srcVertex, int processorIdx) {
        final List<OutboundEdgeStream> outboundStreams = new ArrayList<>();
        for (EdgeDef edge : srcVertex.outboundEdges()) {
            // each edge has an array of conveyors
            // one conveyor per consumer - each conveyor has one queue per producer
            // giving the total number of queues = producers * consumers
            localConveyorMap.computeIfAbsent(edge.edgeId(),
                    e -> createConveyorArray(edge.destVertex().parallelism(),
                            srcVertex.parallelism() + (edge.isDistributed() ? 1 : 0),
                            edge.getConfig().getQueueSize()));
            final Map<Address, ConcurrentConveyor<Object>> memberToSenderConveyorMap =
                    edge.isDistributed() ? memberToSenderConveyorMap(edgeSenderConveyorMap, edge) : null;
            outboundStreams.add(createOutboundEdgeStream(edge, processorIdx, memberToSenderConveyorMap));
        }
        return outboundStreams;
    }

    /**
     * Creates (if absent) for the given edge one sender tasklet per remote member,
     * each with a single conveyor with a number of producer queues feeding it.
     * Populates the {@link #senderMap} and {@link #tasklets} fields.
     */
    private Map<Address, ConcurrentConveyor<Object>> memberToSenderConveyorMap(
            Map<String, Map<Address, ConcurrentConveyor<Object>>> edgeSenderConveyorMap, EdgeDef edge
    ) {
        assert edge.isDistributed() : "Edge is not distributed";
        return edgeSenderConveyorMap.computeIfAbsent(edge.edgeId(), x -> {
            final Map<Address, ConcurrentConveyor<Object>> addrToConveyor = new HashMap<>();
            for (Address destAddr : getRemoteMembers(nodeEngine)) {
                final ConcurrentConveyor<Object> conveyor =
                        createConveyorArray(1, edge.sourceVertex().parallelism(), edge.getConfig().getQueueSize())[0];
                final ConcurrentInboundEdgeStream inboundEdgeStream = createInboundEdgeStream(
                        edge.destOrdinal(), edge.priority(), conveyor);
                final int destVertexId = edge.destVertex().vertexId();
                final SenderTasklet t = new SenderTasklet(inboundEdgeStream, nodeEngine,
                        destAddr, executionId, destVertexId, edge.getConfig().getPacketSizeLimit());
                senderMap.computeIfAbsent(destVertexId, xx -> new HashMap<>())
                         .computeIfAbsent(edge.destOrdinal(), xx -> new HashMap<>())
                         .put(destAddr, t);
                tasklets.add(t);
                addrToConveyor.put(destAddr, conveyor);
            }
            return addrToConveyor;
        });
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentConveyor<Object>[] createConveyorArray(int count, int queueCount, int queueSize) {
        ConcurrentConveyor<Object>[] concurrentConveyors = new ConcurrentConveyor[count];
        Arrays.setAll(concurrentConveyors, i -> {
            QueuedPipe<Object>[] queues = new QueuedPipe[queueCount];
            Arrays.setAll(queues, j -> new OneToOneConcurrentArrayQueue<>(queueSize));
            return concurrentConveyor(DONE_ITEM, queues);
        });
        return concurrentConveyors;
    }

    private OutboundEdgeStream createOutboundEdgeStream(
            EdgeDef edge, int processorIndex, Map<Address, ConcurrentConveyor<Object>> senderConveyorMap
    ) {
        final int totalPtionCount = nodeEngine.getPartitionService().getPartitionCount();
        final int processorCount = edge.destVertex().parallelism();
        final int[][] ptionsPerProcessor = ptionArrgmt.assignPartitionsToProcessors(processorCount, edge.isDistributed());
        final OutboundCollector[] localCollectors = new OutboundCollector[processorCount];
        final ConcurrentConveyor<Object>[] localConveyors = localConveyorMap.get(edge.edgeId());
        Arrays.setAll(localCollectors, n ->
                new ConveyorCollector(localConveyors[n], processorIndex, ptionsPerProcessor[n]));

        final OutboundCollector[] allCollectors;
        if (!edge.isDistributed()) {
            allCollectors = localCollectors;
        } else {
            createIfAbsentReceiverTasklet(edge, ptionsPerProcessor, totalPtionCount);
            // assign remote partitions to outbound data collectors
            final Map<Address, int[]> memberToPartitions = ptionArrgmt.remotePartitionAssignment.get();
            allCollectors = new OutboundCollector[memberToPartitions.size() + 1];
            allCollectors[0] = compositeCollector(localCollectors, edge, totalPtionCount);
            int index = 1;
            for (Map.Entry<Address, int[]> entry : memberToPartitions.entrySet()) {
                allCollectors[index++] = new ConveyorCollectorWithPartition(senderConveyorMap.get(entry.getKey()),
                        processorIndex, entry.getValue());
            }
        }
        return new OutboundEdgeStream(edge.sourceOrdinal(),
                edge.isBuffered() ? Integer.MAX_VALUE : edge.getConfig().getHighWaterMark(),
                compositeCollector(allCollectors, edge, totalPtionCount));
    }

    private void createIfAbsentReceiverTasklet(EdgeDef edge, int[][] ptionsPerProcessor, int totalPtionCount) {
        final ConcurrentConveyor<Object>[] localConveyors = localConveyorMap.get(edge.edgeId());
        receiverMap.computeIfAbsent(edge.destVertex().vertexId(), x -> new HashMap<>())
                   .computeIfAbsent(edge.destOrdinal(), x -> {
                       final OutboundCollector[] collectors = new OutboundCollector[ptionsPerProcessor.length];
                       Arrays.setAll(collectors, n -> new ConveyorCollector(
                               localConveyors[n], localConveyors[n].queueCount() - 1, ptionsPerProcessor[n]));
                       final OutboundCollector collector = compositeCollector(collectors, edge, totalPtionCount);
                       final int senderCount = nodeEngine.getClusterService().getSize() - 1;
                       return new ReceiverTasklet(collector, edge.getConfig().getReceiveWindowMultiplier(),
                               getConfig().getInstanceConfig().getFlowControlPeriodMs(), senderCount);
                   });
    }

    private JetConfig getConfig() {
        JetService service = nodeEngine.getService(JetService.SERVICE_NAME);
        return service.getJetInstance().getConfig();
    }

    private List<InboundEdgeStream> createInboundEdgeStreams(VertexDef srcVertex, int processorIdx) {
        final List<InboundEdgeStream> inboundStreams = new ArrayList<>();
        for (EdgeDef inEdge : srcVertex.inboundEdges()) {
            // each tasklet will have one input conveyor per edge
            // and one InboundEmitter per queue on the conveyor
            final ConcurrentConveyor<Object> conveyor = localConveyorMap.get(inEdge.edgeId())[processorIdx];
            inboundStreams.add(createInboundEdgeStream(inEdge.destOrdinal(), inEdge.priority(), conveyor));
        }
        return inboundStreams;
    }

    private static ConcurrentInboundEdgeStream createInboundEdgeStream(
            int ordinal, int priority, ConcurrentConveyor<Object> conveyor
    ) {
        final InboundEmitter[] emitters = new InboundEmitter[conveyor.queueCount()];
        Arrays.setAll(emitters, n -> new ConveyorEmitter(conveyor, n));
        return new ConcurrentInboundEdgeStream(emitters, ordinal, priority);
    }
}

