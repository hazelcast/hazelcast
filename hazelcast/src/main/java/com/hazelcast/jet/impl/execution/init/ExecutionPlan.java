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
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Edge.RoutingPolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobClassLoaderService;
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
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.jet.impl.util.ObjectWithPartitionId;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.concurrentConveyor;
import static com.hazelcast.jet.config.EdgeConfig.DEFAULT_QUEUE_SIZE;
import static com.hazelcast.jet.core.Edge.DISTRIBUTE_TO_ALL;
import static com.hazelcast.jet.impl.execution.OutboundCollector.compositeCollector;
import static com.hazelcast.jet.impl.execution.TaskletExecutionService.TASKLET_INIT_CLOSE_EXECUTOR_NAME;
import static com.hazelcast.jet.impl.util.ImdgUtil.getMemberConnection;
import static com.hazelcast.jet.impl.util.ImdgUtil.readList;
import static com.hazelcast.jet.impl.util.ImdgUtil.writeList;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefix;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefixedLogger;
import static com.hazelcast.jet.impl.util.Util.doWithClassLoader;
import static com.hazelcast.jet.impl.util.Util.memoize;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toMap;

/**
 * An object sent from a master to members.
 */
public class ExecutionPlan implements IdentifiedDataSerializable {

    // use same size as DEFAULT_QUEUE_SIZE from Edges. In the future we might
    // want to make this configurable
    private static final int SNAPSHOT_QUEUE_SIZE = DEFAULT_QUEUE_SIZE;

    /** Snapshot of partition table used to route items on partitioned edges */
    private Map<Address, int[]> partitionAssignment;

    private JobConfig jobConfig;
    private List<VertexDef> vertices = new ArrayList<>();

    private int memberIndex;
    private int memberCount;
    private long lastSnapshotId;
    private boolean isLightJob;
    private Subject subject;

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
    private transient JobClassLoaderService jobClassLoaderService;
    private transient long executionId;
    private transient DagNodeUtil dagNodeUtil;
    private final transient Set<String> localCollectorsEdges = new HashSet<>();

    // list of unique remote members
    private final transient Supplier<Set<Address>> remoteMembers = memoize(() -> {
        Set<Address> remoteAddresses = new HashSet<>(partitionAssignment.keySet());
        remoteAddresses.remove(nodeEngine.getThisAddress());
        return remoteAddresses;
    });

    ExecutionPlan() {
    }

    ExecutionPlan(Map<Address, int[]> partitionAssignment, JobConfig jobConfig, long lastSnapshotId,
                  int memberIndex, int memberCount, boolean isLightJob, Subject subject) {
        this.partitionAssignment = partitionAssignment;
        this.jobConfig = jobConfig;
        this.lastSnapshotId = lastSnapshotId;
        this.memberIndex = memberIndex;
        this.memberCount = memberCount;
        this.isLightJob = isLightJob;
        this.subject = subject;
    }

    /**
     * A method called on the members as part of the InitExecutionOperation.
     * Creates tasklets, inboxes/outboxes and connects these to make them ready
     * for a later StartExecutionOperation.
     */
    public void initialize(NodeEngineImpl nodeEngine,
                           long jobId,
                           long executionId,
                           @Nonnull SnapshotContext snapshotContext,
                           ConcurrentHashMap<String, File> tempDirectories,
                           InternalSerializationService jobSerializationService) {
        this.nodeEngine = nodeEngine;
        this.jobClassLoaderService =
                ((JetServiceBackend) nodeEngine.getService(JetServiceBackend.SERVICE_NAME)).getJobClassLoaderService();
        this.executionId = executionId;
        initProcSuppliers(jobId, tempDirectories, jobSerializationService);
        initDag(jobSerializationService);

        this.ptionArrgmt = new PartitionArrangement(partitionAssignment, nodeEngine.getThisAddress());
        Set<Integer> higherPriorityVertices = VertexDef.getHigherPriorityVertices(vertices);
        for (Address destAddr : remoteMembers.get()) {
            Connection conn = getMemberConnection(nodeEngine, destAddr);
            if (conn == null) {
                throw new TopologyChangedException("no connection to job participant: " + destAddr);
            }
            memberConnections.put(destAddr, conn);
        }
        dagNodeUtil = new DagNodeUtil(vertices, partitionAssignment.keySet(), nodeEngine.getThisAddress());
        createLocalConveyorsAndSenderReceiverTasklets(jobId, jobSerializationService);

        for (VertexDef vertex : vertices) {
            if (!dagNodeUtil.vertexExists(vertex)) {
                continue;
            }

            ClassLoader processorClassLoader = isLightJob ? null :
                    jobClassLoaderService.getProcessorClassLoader(jobId, vertex.name());
            Collection<? extends Processor> processors = doWithClassLoader(
                    processorClassLoader,
                    () -> createProcessors(vertex, vertex.localParallelism())
            );
            String jobPrefix = prefix(jobConfig.getName(), jobId, vertex.name());

            // create StoreSnapshotTasklet and the queues to it
            ConcurrentConveyor<Object> ssConveyor = null;
            if (!isLightJob) {
                // Note that we create the snapshot queues for all non-light jobs, even if they don't have
                // processing guarantee enabled, because in EE one can request a snapshot also for
                // non-snapshotted jobs.
                @SuppressWarnings("unchecked")
                QueuedPipe<Object>[] snapshotQueues = new QueuedPipe[vertex.localParallelism()];
                Arrays.setAll(snapshotQueues, i -> new OneToOneConcurrentArrayQueue<>(SNAPSHOT_QUEUE_SIZE));
                ssConveyor = ConcurrentConveyor.concurrentConveyor(null, snapshotQueues);
                ILogger storeSnapshotLogger = prefixedLogger(nodeEngine.getLogger(StoreSnapshotTasklet.class), jobPrefix);
                StoreSnapshotTasklet ssTasklet = new StoreSnapshotTasklet(snapshotContext,
                        ConcurrentInboundEdgeStream.create(ssConveyor, 0, 0, true, jobPrefix + "/ssFrom", null),
                        new AsyncSnapshotWriterImpl(nodeEngine, snapshotContext, vertex.name(), memberIndex, memberCount,
                                jobSerializationService),
                        storeSnapshotLogger, vertex.name(), higherPriorityVertices.contains(vertex.vertexId()));
                tasklets.add(ssTasklet);
            }

            int localProcessorIdx = 0;
            for (Processor processor : processors) {
                int globalProcessorIndex = memberIndex * vertex.localParallelism() + localProcessorIdx;
                String processorPrefix = prefix(jobConfig.getName(), jobId, vertex.name(), globalProcessorIndex);
                ILogger logger = prefixedLogger(nodeEngine.getLogger(processor.getClass()), processorPrefix);
                ProcCtx context = new ProcCtx(
                        nodeEngine,
                        jobId,
                        executionId,
                        getJobConfig(),
                        logger,
                        vertex.name(),
                        localProcessorIdx,
                        globalProcessorIndex,
                        isLightJob,
                        partitionAssignment,
                        vertex.localParallelism(),
                        memberIndex,
                        memberCount,
                        tempDirectories,
                        jobSerializationService,
                        subject,
                        processorClassLoader
                );

                List<OutboundEdgeStream> outboundStreams = createOutboundEdgeStreams(
                        vertex, localProcessorIdx);
                List<InboundEdgeStream> inboundStreams = createInboundEdgeStreams(
                        vertex, localProcessorIdx, jobPrefix, globalProcessorIndex);

                OutboundCollector snapshotCollector = ssConveyor == null ? null :
                        new ConveyorCollector(ssConveyor, localProcessorIdx, null);

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
    }

    /**
     * Here is how the communication is done for two vertices in the DAG. Let's assume that we have two vertices V1 and V2 with
     * edge between them. For each of the vertices we create the {@link ProcessorTasklet} instances. The number of the instances
     * depends on the localParallelism of the vertex. When one instance of the {@link ProcessorTasklet} wants to send data
     * over the V1 -> V2 edge:
     * - If {@link ProcessorTasklet} for V1 and {@link ProcessorTasklet} for V2 are on current member, the communication
     *   is done through the local conveyors.
     * - If {@link ProcessorTasklet} for V1 is on remote member, on current member we have to create {@link ReceiverTasklet}
     *   that will receive the data from V1 and pass it to V2. The communication between {@link ReceiverTasklet} and V2
     *   {@link ProcessorTasklet} is done through the local conveyors.
     * - If {@link ProcessorTasklet} for V2 is on remote member, on current member we have to create {@link SenderTasklet}
     *   that will send the data from V1 to V2. The communication between V1 {@link ProcessorTasklet} and {@link SenderTasklet}
     *   is done through the concurrent conveyors.
     * To make it even more clear that's what are our possibilities (http://viz-js.com/):
     *
     * digraph Local {
     *     subgraph cluster_0 {
     *         "V1 ProcessorTasklet" -> "V2 ProcessorTasklet" [label="local conveyor"]
     *         label = "member #1";
     *     }
     * }
     *
     * digraph Remote {
     *     subgraph cluster_0 {
     *         "V1 ProcessorTasklet" -> "V1 SenderTasklet" [label="concurrent conveyor"]
     *         label = "member #1";
     *     }
     *
     *     subgraph cluster_1 {
     *         "V2 ReceiverTasklet" -> "V2 ProcessorTasklet" [label="local conveyor"]
     *         label = "member #2";
     *     }
     *
     *     "V1 SenderTasklet" -> "V2 ReceiverTasklet" [label="network"]
     * }
     */
    private void createLocalConveyorsAndSenderReceiverTasklets(long jobId, InternalSerializationService jobSerializationService) {
        for (VertexDef vertex : vertices) {
            String jobPrefix = prefix(jobConfig.getName(), jobId, vertex.name());

            for (EdgeDef outboundEdge : vertex.outboundEdges()) {
                createLocalConveyorsAndSenderTaskletsForOutbound(outboundEdge, jobPrefix, jobSerializationService);
            }

            for (EdgeDef inboundEdge : vertex.inboundEdges()) {
                createLocalConveyorsAndReceiverTaskletsForInbound(inboundEdge, jobPrefix, jobSerializationService);
            }
        }
    }

    private void createLocalConveyorsAndSenderTaskletsForOutbound(
            EdgeDef outboundEdge,
            String jobPrefix,
            InternalSerializationService jobSerializationService
    ) {
        Set<Address> edgeTargets = dagNodeUtil.getEdgeTargets(outboundEdge);

        // create local connections
        for (Address targetAddress : edgeTargets) {
            if (targetAddress.equals(nodeEngine.getThisAddress())) {
                localCollectorsEdges.add(outboundEdge.edgeId());
                populateLocalConveyorMap(outboundEdge);
            }
        }

        // create remote connections, we need a sender tasklet to send data to a remote member.
        if (!outboundEdge.isLocal()) {
            createSenderTasklets(outboundEdge, jobPrefix, jobSerializationService);
        }
    }

    private void createLocalConveyorsAndReceiverTaskletsForInbound(
            EdgeDef inboundEdge,
            String jobPrefix,
            InternalSerializationService jobSerializationService
    ) {
        Set<Address> edgeSources = dagNodeUtil.getEdgeSources(inboundEdge);

        for (Address sourceAddress : edgeSources) {
            // Local conveyor is always needed either between two processor tasklets, or between processor and receiver tasklets.
            populateLocalConveyorMap(inboundEdge);

            if (sourceAddress.equals(nodeEngine.getThisAddress())) {
                // Local connection on current member, we populate {@link #localCollectorsEdges}
                localCollectorsEdges.add(inboundEdge.edgeId());
            } else {
                // Remote connection, we need a receiver tasklet to fetch data from a remote member.
                createReceiverTasklet(inboundEdge, jobPrefix, jobSerializationService);
            }
        }
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
        out.writeLong(lastSnapshotId);
        out.writeObject(partitionAssignment);
        out.writeBoolean(isLightJob);
        out.writeObject(jobConfig);
        out.writeInt(memberIndex);
        out.writeInt(memberCount);
        ImdgUtil.writeSubject(out, subject);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        vertices = readList(in);
        lastSnapshotId = in.readLong();
        partitionAssignment = in.readObject();
        isLightJob = in.readBoolean();
        jobConfig = in.readObject();
        memberIndex = in.readInt();
        memberCount = in.readInt();
        subject = ImdgUtil.readSubject(in);
    }

    // End implementation of IdentifiedDataSerializable

    private void initProcSuppliers(long jobId,
                                   ConcurrentHashMap<String, File> tempDirectories,
                                   InternalSerializationService jobSerializationService) {
        for (VertexDef vertex : vertices) {
            ClassLoader processorClassLoader = isLightJob ? null :
                    jobClassLoaderService.getProcessorClassLoader(jobId, vertex.name());
            ProcessorSupplier supplier = vertex.processorSupplier();
            String prefix = prefix(jobConfig.getName(), jobId, vertex.name(), "#PS");
            ILogger logger = prefixedLogger(nodeEngine.getLogger(supplier.getClass()), prefix);
            doWithClassLoader(processorClassLoader, () ->
                    supplier.init(new ProcSupplierCtx(
                            nodeEngine,
                            jobId,
                            executionId,
                            jobConfig,
                            logger,
                            vertex.name(),
                            vertex.localParallelism(),
                            vertex.localParallelism() * memberCount,
                            memberIndex,
                            memberCount,
                            isLightJob,
                            partitionAssignment,
                            tempDirectories,
                            jobSerializationService,
                            subject,
                            processorClassLoader
                    )));
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
                .forEach(partitioner -> {
                            if (partitioner instanceof SerializationServiceAware) {
                                ((SerializationServiceAware) partitioner).setSerializationService(jobSerializationService);
                            }
                            partitioner.init(object -> partitionService.getPartitionId(jobSerializationService.toData(object)));
                        }
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

    private List<OutboundEdgeStream> createOutboundEdgeStreams(VertexDef vertex, int processorIdx) {
        List<OutboundEdgeStream> outboundStreams = new ArrayList<>();
        for (EdgeDef edge : vertex.outboundEdges()) {
            OutboundCollector outboundCollector = createOutboundCollector(edge, processorIdx);
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
    private OutboundCollector createOutboundCollector(EdgeDef edge, int processorIndex) {
        if (edge.routingPolicy() == RoutingPolicy.ISOLATED && !edge.isLocal()) {
            throw new IllegalArgumentException("Isolated edges must be local: " + edge);
        }

        int totalPartitionCount = nodeEngine.getPartitionService().getPartitionCount();
        int[][] partitionsPerProcessor = getLocalPartitionDistribution(edge, edge.destVertex().localParallelism());

        OutboundCollector localCollector = null;

        if (localCollectorsEdges.contains(edge.edgeId())) {
            localCollector = createLocalOutboundCollector(
                    edge,
                    processorIndex,
                    totalPartitionCount,
                    partitionsPerProcessor
            );

            if (edge.isLocal()) {
                return localCollector;
            }
        }

        OutboundCollector[] remoteCollectors = createRemoteOutboundCollectors(edge, processorIndex);
        if (localCollector == null) {
            // If we have no localCollector and exactly one remoteCollector then we want it to be opaque with
            // OutboundCollector.Partitioned, that's why we set fastReturnSingleCollector to false.
            return compositeCollector(remoteCollectors, edge, totalPartitionCount, false, true);
        }

        // in a distributed edge, collectors[0] is the composite of local collector, and
        // collectors[n] where n > 0 is a collector pointing to a remote member _n_.
        OutboundCollector[] collectors = new OutboundCollector[remoteCollectors.length + 1];
        collectors[0] = localCollector;
        System.arraycopy(remoteCollectors, 0, collectors, 1, collectors.length - 1);
        return compositeCollector(collectors, edge, totalPartitionCount, false, false);
    }

    private void populateLocalConveyorMap(EdgeDef edge) {
        if (localConveyorMap.containsKey(edge.edgeId())) {
            return;
        }

        int upstreamParallelism = edge.sourceVertex().localParallelism();
        int downstreamParallelism = edge.destVertex().localParallelism();
        int queueSize = edge.getConfig().getQueueSize();
        int numRemoteMembers = dagNodeUtil.numRemoteSources(edge);

        if (edge.routingPolicy() == RoutingPolicy.ISOLATED) {
            int queueCount = upstreamParallelism / downstreamParallelism;
            int remainder = upstreamParallelism % downstreamParallelism;

            localConveyorMap.put(edge.edgeId(), Stream.concat(
                    Arrays.stream(createConveyorArray(remainder, queueCount + 1, queueSize)),
                    Arrays.stream(createConveyorArray(
                            downstreamParallelism - remainder, Math.max(1, queueCount), queueSize
                    ))).toArray((IntFunction<ConcurrentConveyor<Object>[]>) ConcurrentConveyor[]::new)
            );
        } else {
            int queueCount = !localCollectorsEdges.contains(edge.edgeId()) ? numRemoteMembers :
                    upstreamParallelism + (!edge.isLocal() ? numRemoteMembers : 0);
            localConveyorMap.put(edge.edgeId(), createConveyorArray(downstreamParallelism, queueCount, queueSize));
        }
    }

    private OutboundCollector createLocalOutboundCollector(
            EdgeDef edge,
            int processorIndex,
            int totalPartitionCount,
            int[][] partitionsPerProcessor
    ) {
        int upstreamParallelism = edge.sourceVertex().localParallelism();
        int downstreamParallelism = edge.destVertex().localParallelism();

        ConcurrentConveyor<Object>[] localConveyors = localConveyorMap.get(edge.edgeId());
        if (edge.routingPolicy() == RoutingPolicy.ISOLATED) {
            OutboundCollector[] localCollectors = IntStream.range(0, downstreamParallelism)
                            .filter(i -> i % upstreamParallelism == processorIndex % downstreamParallelism)
                            .mapToObj(i -> new ConveyorCollector(localConveyors[i],
                                    processorIndex / downstreamParallelism, null))
                            .toArray(OutboundCollector[]::new);
            return compositeCollector(localCollectors, edge, totalPartitionCount, true, false);
        } else {
            OutboundCollector[] localCollectors = new OutboundCollector[downstreamParallelism];
            Arrays.setAll(
                    localCollectors,
                    n -> new ConveyorCollector(localConveyors[n], processorIndex, partitionsPerProcessor[n])
            );
            return compositeCollector(localCollectors, edge, totalPartitionCount, true, false);
        }
    }

    private OutboundCollector[] createRemoteOutboundCollectors(EdgeDef edge, int processorIndex) {
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

        Map<Address, ConcurrentConveyor<Object>> senderConveyorMap = edgeSenderConveyorMap.get(edge.edgeId());

        // assign remote partitions to outbound data collectors
        Address distributeTo = edge.getDistributedTo();
        Map<Address, int[]> memberToPartitions = distributeTo.equals(DISTRIBUTE_TO_ALL)
                ? ptionArrgmt.getRemotePartitionAssignment()
                : ptionArrgmt.remotePartitionAssignmentToOne(distributeTo);

        List<OutboundCollector> remoteCollectors = new ArrayList<>(memberToPartitions.size());
        for (Map.Entry<Address, int[]> entry : memberToPartitions.entrySet()) {
            Address memberAddress = entry.getKey();
            int[] memberPartitions = entry.getValue();
            ConcurrentConveyor<Object> conveyor = senderConveyorMap.get(memberAddress);
            if (conveyor == null) {
                continue;
            }
            remoteCollectors.add(new ConveyorCollectorWithPartition(conveyor, processorIndex, memberPartitions));
        }
        return remoteCollectors.toArray(new OutboundCollector[0]);
    }

    /**
     * Creates (if absent) for the given edge one sender tasklet per remote member,
     * each with a single conveyor with a number of producer queues feeding it.
     * Populates the {@link #senderMap} and {@link #tasklets} and {@link #edgeSenderConveyorMap} fields.
     */
    private void createSenderTasklets(
            EdgeDef edge,
            String jobPrefix,
            InternalSerializationService jobSerializationService
    ) {
        assert !edge.isLocal() : "Edge is not distributed";

        if (edgeSenderConveyorMap.containsKey(edge.edgeId())) {
            return;
        }

        int destVertexId = edge.destVertex().vertexId();
        Map<Address, ConcurrentConveyor<Object>> addrToConveyor = new HashMap<>();
        @SuppressWarnings("unchecked")
        ComparatorEx<Object> origComparator = (ComparatorEx<Object>) edge.getOrderComparator();
        ComparatorEx<ObjectWithPartitionId> adaptedComparator = origComparator == null ? null
                : (l, r) -> origComparator.compare(l.getItem(), r.getItem());

        for (Address destAddr : dagNodeUtil.getEdgeTargets(edge)) {
            if (destAddr.equals(nodeEngine.getThisAddress())) {
                continue;
            }

            ConcurrentConveyor<Object> conveyor = createConveyorArray(
                    1, edge.sourceVertex().localParallelism(), edge.getConfig().getQueueSize())[0];

            InboundEdgeStream inboundEdgeStream = newEdgeStream(edge, conveyor,
                    jobPrefix + "/toVertex:" + edge.destVertex().name() + "-toMember:" + destAddr,
                    adaptedComparator);
            SenderTasklet t = new SenderTasklet(inboundEdgeStream, nodeEngine, destAddr,
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

        edgeSenderConveyorMap.put(edge.edgeId(), addrToConveyor);
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

    private void createReceiverTasklet(
            EdgeDef inboundEdge,
            String jobPrefix,
            InternalSerializationService jobSerializationService
    ) {
        int totalPartitionCount = nodeEngine.getPartitionService().getPartitionCount();
        int[][] partitionsPerProcessor = getLocalPartitionDistribution(inboundEdge,
                inboundEdge.destVertex().localParallelism());
        createReceiverTasklet(inboundEdge, jobPrefix, partitionsPerProcessor, totalPartitionCount,
                jobSerializationService);
    }

    /**
     * Creates receiver tasklet and populates the {@link #receiverMap} for an edge.
     */
    private void createReceiverTasklet(EdgeDef edge,
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
                        if (!dagNodeUtil.getEdgeSources(edge).contains(addr)) {
                            continue;
                        }

                        final OutboundCollector[] collectors = new OutboundCollector[ptionsPerProcessor.length];
                        // assign the queues starting from end
                        final int queueOffset = --offset;
                        Arrays.setAll(collectors, n -> new ConveyorCollector(
                                localConveyors[n], localConveyors[n].queueCount() + queueOffset,
                                ptionsPerProcessor[n]));
                        final OutboundCollector collector = compositeCollector(collectors, edge, totalPtionCount, true, false);
                        ReceiverTasklet receiverTasklet = new ReceiverTasklet(
                                collector, jobSerializationService,
                                edge.getConfig().getReceiveWindowMultiplier(),
                                getJetConfig().getFlowControlPeriodMs(),
                                nodeEngine.getLoggingService(), addr, edge.destOrdinal(), edge.destVertex().name(),
                                memberConnections.get(addr), jobPrefix);
                        addrToTasklet.put(addr, receiverTasklet);
                        tasklets.add(receiverTasklet);
                    }
                    return addrToTasklet;
                });
    }

    private JetConfig getJetConfig() {
        return nodeEngine.getConfig().getJetConfig();
    }

    private List<InboundEdgeStream> createInboundEdgeStreams(VertexDef srcVertex, int localProcessorIdx,
                                                             String jobPrefix, int globalProcessorIdx) {
        final List<InboundEdgeStream> inboundStreams = new ArrayList<>();
        for (EdgeDef inEdge : srcVertex.inboundEdges()) {
            if (dagNodeUtil.getEdgeSources(inEdge).isEmpty()) {
                continue;
            }
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

    public List<VertexDef> getVertices() {
        return unmodifiableList(vertices);
    }
}
