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
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Edge.RoutingPolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.ConcurrentInboundEdgeStream;
import com.hazelcast.jet.impl.execution.ConveyorCollector;
import com.hazelcast.jet.impl.execution.ConveyorCollectorWithPartition;
import com.hazelcast.jet.impl.execution.InboundEdgeStream;
import com.hazelcast.jet.impl.execution.OutboundCollector;
import com.hazelcast.jet.impl.execution.OutboundEdgeStream;
import com.hazelcast.jet.impl.execution.ProcessorTasklet;
import com.hazelcast.jet.impl.execution.ReceiverTasklet;
import com.hazelcast.jet.impl.execution.SenderTasklet;
import com.hazelcast.jet.impl.execution.SnapshotContext;
import com.hazelcast.jet.impl.execution.StoreSnapshotTasklet;
import com.hazelcast.jet.impl.execution.Tasklet;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl;
import com.hazelcast.jet.impl.util.ObjectWithPartitionId;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.concurrentConveyor;
import static com.hazelcast.jet.config.EdgeConfig.DEFAULT_QUEUE_SIZE;
import static com.hazelcast.jet.core.Edge.DISTRIBUTE_TO_ALL;
import static com.hazelcast.jet.impl.execution.OutboundCollector.compositeCollector;
import static com.hazelcast.jet.impl.execution.TaskletExecutionService.TASKLET_INIT_CLOSE_EXECUTOR_NAME;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.ImdgUtil.getMemberConnection;
import static com.hazelcast.jet.impl.util.ImdgUtil.readList;
import static com.hazelcast.jet.impl.util.ImdgUtil.writeList;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefix;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefixedLogger;
import static com.hazelcast.jet.impl.util.Util.getJetInstance;
import static com.hazelcast.jet.impl.util.Util.memoize;
import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * An object sent from a master to members.
 */
public class ExecutionPlan implements IdentifiedDataSerializable {

    // use same size as DEFAULT_QUEUE_SIZE from Edges. In the future we might
    // want to make this configurable
    private static final int SNAPSHOT_QUEUE_SIZE = DEFAULT_QUEUE_SIZE;

    /** Snapshot of partition table used to route items on partitioned edges */
    private Address[] partitionOwners;

    private JobConfig jobConfig;
    private List<VertexDef> vertices = new ArrayList<>();

    private int memberIndex;
    private int memberCount;
    private long lastSnapshotId;

    // *** Transient state below, used during #initialize() ***

    private final transient List<Tasklet> tasklets = new ArrayList<>();

    private final transient Map<Address, Connection> memberConnections = new HashMap<>();

    /** dest vertex id --> dest ordinal --> sender addr -> receiver tasklet */
    private final transient Map<Integer, Map<Integer, Map<Address, ReceiverTasklet>>> receiverMap = new HashMap<>();

    /** dest vertex id --> dest ordinal --> dest addr --> sender tasklet */
    private final transient Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap = new HashMap<>();

    private final transient Map<String, ConcurrentConveyor<Object>[]> localConveyorMap = new HashMap<>();
    private final transient Map<String, Map<Address, ConcurrentConveyor<Object>>> edgeSenderConveyorMap = new HashMap<>();
    private final transient List<Processor> processors = new ArrayList<>();

    private transient PartitionArrangement ptionArrgmt;

    private transient NodeEngineImpl nodeEngine;
    private transient long executionId;

    // list of unique remote members
    private final transient Supplier<Set<Address>> remoteMembers = memoize(() ->
            Arrays.stream(partitionOwners)
                  .filter(a -> !a.equals(nodeEngine.getThisAddress()))
                  .collect(Collectors.toSet())
    );

    ExecutionPlan() {
    }

    ExecutionPlan(Address[] partitionOwners, JobConfig jobConfig, long lastSnapshotId,
                  int memberIndex, int memberCount) {
        this.partitionOwners = partitionOwners;
        this.jobConfig = jobConfig;
        this.lastSnapshotId = lastSnapshotId;
        this.memberIndex = memberIndex;
        this.memberCount = memberCount;
    }

    /**
     * A method called on the members as part of the InitExecutionOperation.
     * Creates tasklets, inboxes/outboxes and connects these to make them ready
     * for a later StartExecutionOperation.
     */
    public void initialize(NodeEngine nodeEngine,
                           long jobId,
                           long executionId,
                           SnapshotContext snapshotContext,
                           ConcurrentHashMap<String, File> tempDirectories,
                           InternalSerializationService jobSerializationService) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.executionId = executionId;
        initProcSuppliers(jobId, tempDirectories, jobSerializationService);
        initDag(jobSerializationService);

        this.ptionArrgmt = new PartitionArrangement(partitionOwners, nodeEngine.getThisAddress());
        JetInstance instance = getJetInstance(nodeEngine);
        Set<Integer> higherPriorityVertices = VertexDef.getHigherPriorityVertices(vertices);
        for (Address destAddr : remoteMembers.get()) {
            memberConnections.put(destAddr, getMemberConnection(nodeEngine, destAddr));
        }
        for (VertexDef vertex : vertices) {
            Collection<? extends Processor> processors = createProcessors(vertex, vertex.localParallelism());

            // create StoreSnapshotTasklet and the queues to it
            @SuppressWarnings("unchecked")
            QueuedPipe<Object>[] snapshotQueues = new QueuedPipe[vertex.localParallelism()];
            Arrays.setAll(snapshotQueues, i -> new OneToOneConcurrentArrayQueue<>(SNAPSHOT_QUEUE_SIZE));
            ConcurrentConveyor<Object> ssConveyor = ConcurrentConveyor.concurrentConveyor(null, snapshotQueues);
            String jobPrefix = prefix(jobConfig.getName(), jobId, vertex.name());
            ILogger storeSnapshotLogger = prefixedLogger(nodeEngine.getLogger(StoreSnapshotTasklet.class), jobPrefix);
            StoreSnapshotTasklet ssTasklet = new StoreSnapshotTasklet(snapshotContext,
                    ConcurrentInboundEdgeStream.create(ssConveyor, 0, 0, true, jobPrefix + "/ssFrom", null),
                    new AsyncSnapshotWriterImpl(nodeEngine, snapshotContext, vertex.name(), memberIndex, memberCount,
                            jobSerializationService),
                    storeSnapshotLogger, vertex.name(), higherPriorityVertices.contains(vertex.vertexId()));
            tasklets.add(ssTasklet);

            int localProcessorIdx = 0;
            for (Processor processor : processors) {
                int globalProcessorIndex = memberIndex * vertex.localParallelism() + localProcessorIdx;
                String processorPrefix = prefix(jobConfig.getName(), jobId, vertex.name(), globalProcessorIndex);
                ILogger logger = prefixedLogger(nodeEngine.getLogger(processor.getClass()), processorPrefix);
                ProcCtx context = new ProcCtx(
                        instance,
                        jobId,
                        executionId,
                        getJobConfig(),
                        logger,
                        vertex.name(),
                        localProcessorIdx,
                        globalProcessorIndex,
                        jobConfig.getProcessingGuarantee(),
                        vertex.localParallelism(),
                        memberIndex,
                        memberCount,
                        tempDirectories,
                        jobSerializationService
                );

                // createOutboundEdgeStreams() populates localConveyorMap and edgeSenderConveyorMap.
                // Also populates instance fields: senderMap, receiverMap, tasklets.
                List<OutboundEdgeStream> outboundStreams = createOutboundEdgeStreams(
                        vertex, localProcessorIdx, jobPrefix, jobSerializationService);
                List<InboundEdgeStream> inboundStreams = createInboundEdgeStreams(
                        vertex, localProcessorIdx, jobPrefix, globalProcessorIndex);

                OutboundCollector snapshotCollector = new ConveyorCollector(ssConveyor, localProcessorIdx, null);

                // vertices which are only used for snapshot restore will not be marked as "source=true" in metrics
                // also do not consider snapshot restore edges for determining source tag
                boolean isSource = vertex.inboundEdges().stream().allMatch(EdgeDef::isSnapshotRestoreEdge)
                        && !vertex.isSnapshotVertex();

                ProcessorTasklet processorTasklet = new ProcessorTasklet(context,
                        nodeEngine.getExecutionService().getExecutor(TASKLET_INIT_CLOSE_EXECUTOR_NAME),
                        jobSerializationService, processor, inboundStreams, outboundStreams, snapshotContext,
                        snapshotCollector, isSource);
                tasklets.add(processorTasklet);
                this.processors.add(processor);
                localProcessorIdx++;
            }
        }
        List<ReceiverTasklet> allReceivers = receiverMap.values().stream()
                                                        .flatMap(o -> o.values().stream())
                                                        .flatMap(a -> a.values().stream())
                                                        .collect(toList());

        tasklets.addAll(allReceivers);
    }

    public List<ProcessorSupplier> getProcessorSuppliers() {
        return toList(vertices, VertexDef::processorSupplier);
    }

    public Map<Integer, Map<Integer, Map<Address, ReceiverTasklet>>> getReceiverMap() {
        return receiverMap;
    }

    public Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> getSenderMap() {
        return senderMap;
    }

    public List<Tasklet> getTasklets() {
        return tasklets;
    }

    public JobConfig getJobConfig() {
        return jobConfig;
    }

    void addVertex(VertexDef vertex) {
        vertices.add(vertex);
    }

    // Implementation of IdentifiedDataSerializable

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.EXECUTION_PLAN;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeList(out, vertices);
        out.writeInt(partitionOwners.length);
        out.writeLong(lastSnapshotId);
        for (Address address : partitionOwners) {
            out.writeObject(address);
        }
        out.writeObject(jobConfig);
        out.writeInt(memberIndex);
        out.writeInt(memberCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        vertices = readList(in);
        int len = in.readInt();
        partitionOwners = new Address[len];
        lastSnapshotId = in.readLong();
        for (int i = 0; i < len; i++) {
            partitionOwners[i] = in.readObject();
        }
        jobConfig = in.readObject();
        memberIndex = in.readInt();
        memberCount = in.readInt();
    }

    // End implementation of IdentifiedDataSerializable

    private void initProcSuppliers(long jobId,
                                   ConcurrentHashMap<String, File> tempDirectories,
                                   InternalSerializationService jobSerializationService) {
        JetService service = nodeEngine.getService(JetService.SERVICE_NAME);

        for (VertexDef vertex : vertices) {
            ProcessorSupplier supplier = vertex.processorSupplier();
            String prefix = prefix(jobConfig.getName(), jobId, vertex.name(), "#PS");
            ILogger logger = prefixedLogger(nodeEngine.getLogger(supplier.getClass()), prefix);
            try {
                supplier.init(new ProcSupplierCtx(
                        service.getJetInstance(),
                        jobId,
                        executionId,
                        jobConfig,
                        logger,
                        vertex.name(),
                        vertex.localParallelism(),
                        vertex.localParallelism() * memberCount,
                        memberIndex,
                        memberCount,
                        jobConfig.getProcessingGuarantee(),
                        tempDirectories,
                        jobSerializationService
                ));
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        }
    }

    private void initDag(InternalSerializationService jobSerializationService) {
        final Map<Integer, VertexDef> vMap = vertices.stream().collect(toMap(VertexDef::vertexId, v -> v));
        for (VertexDef v : vertices) {
            v.inboundEdges().forEach(e -> e.initTransientFields(vMap, v, false));
            v.outboundEdges().forEach(e -> e.initTransientFields(vMap, v, true));
        }
        final IPartitionService partitionService = nodeEngine.getPartitionService();
        vertices.stream()
                .map(VertexDef::outboundEdges)
                .flatMap(List::stream)
                .map(EdgeDef::partitioner)
                .filter(Objects::nonNull)
                .forEach(partitioner ->
                        partitioner.init(object -> partitionService.getPartitionId(jobSerializationService.toData(object)))
                );
    }

    private static Collection<? extends Processor> createProcessors(VertexDef vertexDef, int parallelism) {
        final Collection<? extends Processor> processors = vertexDef.processorSupplier().get(parallelism);
        if (processors.size() != parallelism) {
            throw new JetException("ProcessorSupplier failed to return the requested number of processors." +
                    " Requested: " + parallelism + ", returned: " + processors.size());
        }
        return processors;
    }

    private List<OutboundEdgeStream> createOutboundEdgeStreams(
            VertexDef vertex,
            int processorIdx,
            String jobPrefix,
            InternalSerializationService jobSerializationService
    ) {
        List<OutboundEdgeStream> outboundStreams = new ArrayList<>();
        for (EdgeDef edge : vertex.outboundEdges()) {
            OutboundCollector outboundCollector =
                    createOutboundCollector(edge, processorIdx, jobPrefix, jobSerializationService);
            OutboundEdgeStream outboundEdgeStream = new OutboundEdgeStream(edge.sourceOrdinal(), outboundCollector);
            outboundStreams.add(outboundEdgeStream);
        }
        return outboundStreams;
    }

    /**
     * Each edge is represented by an array of conveyors between the producers and consumers.
     * There are as many conveyors as there are consumers.
     * Each conveyor has one queue per producer.
     *
     * For a distributed edge, there is one additional producer per member represented
     * by the ReceiverTasklet.
     */
    private OutboundCollector createOutboundCollector(
            EdgeDef edge,
            int processorIndex,
            String jobPrefix,
            InternalSerializationService jobSerializationService
    ) {
        if (edge.routingPolicy() == RoutingPolicy.ISOLATED && !edge.isLocal()) {
            throw new IllegalArgumentException("Isolated edges must be local: " + edge);
        }

        int totalPartitionCount = nodeEngine.getPartitionService().getPartitionCount();
        int[][] partitionsPerProcessor = getLocalPartitionDistribution(edge, edge.destVertex().localParallelism());

        OutboundCollector localCollector = createLocalOutboundCollector(
                edge,
                processorIndex,
                totalPartitionCount,
                partitionsPerProcessor
        );
        if (edge.isLocal()) {
            return localCollector;
        }

        OutboundCollector[] remoteCollectors = createRemoteOutboundCollectors(
                edge,
                jobPrefix,
                processorIndex,
                totalPartitionCount,
                partitionsPerProcessor,
                jobSerializationService
        );

        // in a distributed edge, collectors[0] is the composite of local collector, and
        // collectors[n] where n > 0 is a collector pointing to a remote member _n_.
        OutboundCollector[] collectors = new OutboundCollector[remoteCollectors.length + 1];
        collectors[0] = localCollector;
        System.arraycopy(remoteCollectors, 0, collectors, 1, collectors.length - 1);
        return compositeCollector(collectors, edge, totalPartitionCount, false);
    }

    private OutboundCollector createLocalOutboundCollector(
            EdgeDef edge,
            int processorIndex,
            int totalPartitionCount,
            int[][] partitionsPerProcessor
    ) {
        int upstreamParallelism = edge.sourceVertex().localParallelism();
        int downstreamParallelism = edge.destVertex().localParallelism();
        int queueSize = edge.getConfig().getQueueSize();
        int numRemoteMembers = ptionArrgmt.getRemotePartitionAssignment().size();

        if (edge.routingPolicy() == RoutingPolicy.ISOLATED) {
            ConcurrentConveyor<Object>[] localConveyors = localConveyorMap.computeIfAbsent(
                    edge.edgeId(),
                    edgeId -> {
                        int queueCount = upstreamParallelism / downstreamParallelism;
                        int remainder = upstreamParallelism % downstreamParallelism;
                        return Stream.concat(
                                Arrays.stream(createConveyorArray(remainder, queueCount + 1, queueSize)),
                                Arrays.stream(createConveyorArray(
                                        downstreamParallelism - remainder, Math.max(1, queueCount), queueSize
                                ))).toArray((IntFunction<ConcurrentConveyor<Object>[]>) ConcurrentConveyor[]::new);
                    });

            OutboundCollector[] localCollectors = IntStream.range(0, downstreamParallelism)
                            .filter(i -> i % upstreamParallelism == processorIndex % downstreamParallelism)
                            .mapToObj(i -> new ConveyorCollector(localConveyors[i],
                                    processorIndex / downstreamParallelism, null))
                            .toArray(OutboundCollector[]::new);
            return compositeCollector(localCollectors, edge, totalPartitionCount, true);
        } else {
            ConcurrentConveyor<Object>[] localConveyors = localConveyorMap.computeIfAbsent(
                    edge.edgeId(),
                    edgeId -> {
                        int queueCount = upstreamParallelism + (!edge.isLocal() ? numRemoteMembers : 0);
                        return createConveyorArray(downstreamParallelism, queueCount, queueSize);
                    }
            );

            OutboundCollector[] localCollectors = new OutboundCollector[downstreamParallelism];
            Arrays.setAll(
                    localCollectors,
                    n -> new ConveyorCollector(localConveyors[n], processorIndex, partitionsPerProcessor[n])
            );
            return compositeCollector(localCollectors, edge, totalPartitionCount, true);
        }
    }

    private OutboundCollector[] createRemoteOutboundCollectors(
            EdgeDef edge,
            String jobPrefix,
            int processorIndex,
            int totalPartitionCount,
            int[][] partitionsPerProcessor,
            InternalSerializationService jobSerializationService
    ) {
        // the distributed-to-one edge must be partitioned and the target member must be present
        if (!edge.getDistributedTo().equals(DISTRIBUTE_TO_ALL)) {
            if (edge.routingPolicy() != RoutingPolicy.PARTITIONED) {
                throw new JetException("An edge distributing to a specific member must be partitioned: " + edge);
            }
            if (!ptionArrgmt.getRemotePartitionAssignment().containsKey(edge.getDistributedTo())
                && !edge.getDistributedTo().equals(nodeEngine.getThisAddress())) {
                throw new JetException("The target member of an edge is not present in the cluster or is a lite member: "
                                       + edge);
            }
        }

        Map<Address, ConcurrentConveyor<Object>> senderConveyorMap = memberToSenderConveyorMap(
                edgeSenderConveyorMap,
                edge,
                jobPrefix,
                jobSerializationService
        );
        createIfAbsentReceiverTasklet(edge, jobPrefix, partitionsPerProcessor,
                totalPartitionCount, jobSerializationService);

        // assign remote partitions to outbound data collectors
        Address distributeTo = edge.getDistributedTo();
        Map<Address, int[]> memberToPartitions = distributeTo.equals(DISTRIBUTE_TO_ALL)
                ? ptionArrgmt.getRemotePartitionAssignment()
                : ptionArrgmt.remotePartitionAssignmentToOne(distributeTo);

        OutboundCollector[] remoteCollectors = new OutboundCollector[memberToPartitions.size()];
        int index = 0;
        for (Map.Entry<Address, int[]> entry : memberToPartitions.entrySet()) {
            Address memberAddress = entry.getKey();
            int[] memberPartitions = entry.getValue();
            ConcurrentConveyor<Object> conveyor = senderConveyorMap.get(memberAddress);

            remoteCollectors[index++] = new ConveyorCollectorWithPartition(conveyor, processorIndex, memberPartitions);
        }
        return remoteCollectors;
    }

    /**
     * Creates (if absent) for the given edge one sender tasklet per remote member,
     * each with a single conveyor with a number of producer queues feeding it.
     * Populates the {@link #senderMap} and {@link #tasklets} fields.
     */
    private Map<Address, ConcurrentConveyor<Object>> memberToSenderConveyorMap(
            Map<String, Map<Address, ConcurrentConveyor<Object>>> edgeSenderConveyorMap,
            EdgeDef edge,
            String jobPrefix,
            InternalSerializationService jobSerializationService
    ) {
        assert !edge.isLocal() : "Edge is not distributed";

        return edgeSenderConveyorMap.computeIfAbsent(edge.edgeId(), x -> {
            final Map<Address, ConcurrentConveyor<Object>> addrToConveyor = new HashMap<>();
            for (Address destAddr : remoteMembers.get()) {
                final ConcurrentConveyor<Object> conveyor = createConveyorArray(
                        1, edge.sourceVertex().localParallelism(), edge.getConfig().getQueueSize())[0];
                @SuppressWarnings("unchecked")
                ComparatorEx<Object> origComparator = (ComparatorEx<Object>) edge.getOrderComparator();
                ComparatorEx<ObjectWithPartitionId> adaptedComparator = origComparator == null ? null
                        : (l, r) -> origComparator.compare(l.getItem(), r.getItem());

                final InboundEdgeStream inboundEdgeStream = newEdgeStream(edge, conveyor,
                        jobPrefix + "/toVertex:" + edge.destVertex().name() + "-toMember:" + destAddr,
                        adaptedComparator);
                final int destVertexId = edge.destVertex().vertexId();
                final SenderTasklet t = new SenderTasklet(inboundEdgeStream, nodeEngine, destAddr,
                        memberConnections.get(destAddr),
                        destVertexId, edge.getConfig().getPacketSizeLimit(), executionId,
                        edge.sourceVertex().name(), edge.sourceOrdinal(), jobSerializationService
                );
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
            return concurrentConveyor(null, queues);
        });
        return concurrentConveyors;
    }

    /**
     * Return the partition distribution for local conveyors. The first
     * dimension in the result is the processor index, at that index is the
     * list of partitions.
     */
    private int[][] getLocalPartitionDistribution(EdgeDef edge, int downstreamParallelism) {
        if (!edge.routingPolicy().equals(RoutingPolicy.PARTITIONED)) {
            // the edge is not partitioned, use `null` for each processor
            return new int[downstreamParallelism][];
        }

        if (edge.isLocal() || nodeEngine.getThisAddress().equals(edge.getDistributedTo())) {
            // the edge is local-partitioned or it is distributed to one member and this member is the target
            return ptionArrgmt.assignPartitionsToProcessors(downstreamParallelism, false);
        }

        if (edge.getDistributedTo().equals(DISTRIBUTE_TO_ALL)) {
            // the edge is distributed to all members, every member handles a subset of the partitions
            return ptionArrgmt.assignPartitionsToProcessors(downstreamParallelism, true);
        }

        // the edge is distributed to a specific member and this member is NOT the target member,
        // we assign zero partitions to each processor
        int[][] res = new int[downstreamParallelism][];
        Arrays.fill(res, new int[0]);
        return res;
    }

    private void createIfAbsentReceiverTasklet(EdgeDef edge,
                                               String jobPrefix,
                                               int[][] ptionsPerProcessor,
                                               int totalPtionCount,
                                               InternalSerializationService jobSerializationService) {
        final ConcurrentConveyor<Object>[] localConveyors = localConveyorMap.get(edge.edgeId());

        receiverMap.computeIfAbsent(edge.destVertex().vertexId(), x -> new HashMap<>())
                   .computeIfAbsent(edge.destOrdinal(), x -> {
                       Map<Address, ReceiverTasklet> addrToTasklet = new HashMap<>();
                       //create a receiver per address
                       int offset = 0;
                       for (Address addr : ptionArrgmt.getRemotePartitionAssignment().keySet()) {
                           final OutboundCollector[] collectors = new OutboundCollector[ptionsPerProcessor.length];
                           // assign the queues starting from end
                           final int queueOffset = --offset;
                           Arrays.setAll(collectors, n -> new ConveyorCollector(
                                   localConveyors[n], localConveyors[n].queueCount() + queueOffset,
                                   ptionsPerProcessor[n]));
                           final OutboundCollector collector = compositeCollector(collectors, edge, totalPtionCount, true);
                           ReceiverTasklet receiverTasklet = new ReceiverTasklet(
                                   collector, jobSerializationService,
                                   edge.getConfig().getReceiveWindowMultiplier(),
                                   getConfig().getInstanceConfig().getFlowControlPeriodMs(),
                                   nodeEngine.getLoggingService(), addr, edge.destOrdinal(), edge.destVertex().name(),
                                   memberConnections.get(addr), jobPrefix);
                           addrToTasklet.put(addr, receiverTasklet);
                       }
                       return addrToTasklet;
                   });
    }

    private JetConfig getConfig() {
        JetService service = nodeEngine.getService(JetService.SERVICE_NAME);
        return service.getJetInstance().getConfig();
    }

    private List<InboundEdgeStream> createInboundEdgeStreams(VertexDef srcVertex, int localProcessorIdx,
                                                             String jobPrefix, int globalProcessorIdx) {
        final List<InboundEdgeStream> inboundStreams = new ArrayList<>();
        for (EdgeDef inEdge : srcVertex.inboundEdges()) {
            // each tasklet has one input conveyor per edge
            final ConcurrentConveyor<Object> conveyor = localConveyorMap.get(inEdge.edgeId())[localProcessorIdx];
            inboundStreams.add(newEdgeStream(inEdge, conveyor,
                    jobPrefix + "#" + globalProcessorIdx, inEdge.getOrderComparator()));
        }
        return inboundStreams;
    }

    private InboundEdgeStream newEdgeStream(
            EdgeDef inEdge, ConcurrentConveyor<Object> conveyor, String debugName, ComparatorEx<?> comparator
    ) {
        return ConcurrentInboundEdgeStream.create(conveyor, inEdge.destOrdinal(), inEdge.priority(),
                jobConfig.getProcessingGuarantee() == ProcessingGuarantee.EXACTLY_ONCE,
                debugName, comparator);
    }

    public List<Processor> getProcessors() {
        return processors;
    }

    public long lastSnapshotId() {
        return lastSnapshotId;
    }

    public int getStoreSnapshotTaskletCount() {
        return (int) tasklets.stream()
                             .filter(t -> t instanceof StoreSnapshotTasklet)
                             .count();
    }

    public int getProcessorTaskletCount() {
        return (int) tasklets.stream()
                             .filter(t -> t instanceof ProcessorTasklet)
                             .count();
    }

    public int getHigherPriorityVertexCount() {
        return VertexDef.getHigherPriorityVertices(vertices).size();
    }

    // for test
    List<VertexDef> getVertices() {
        return vertices;
    }
}
