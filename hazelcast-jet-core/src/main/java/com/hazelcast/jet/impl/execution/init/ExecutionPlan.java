/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.concurrentConveyor;
import static com.hazelcast.jet.impl.execution.OutboundCollector.compositeCollector;
import static com.hazelcast.jet.impl.util.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.util.Util.getRemoteMembers;
import static com.hazelcast.jet.impl.util.Util.readList;
import static com.hazelcast.jet.impl.util.Util.writeList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ExecutionPlan implements IdentifiedDataSerializable {
    private final List<ProcessorSupplier> procSuppliers = new ArrayList<>();
    private final List<Tasklet> tasklets = new ArrayList<>();
    // vertex id --> ordinal --> receiver tasklet
    private final Map<Integer, Map<Integer, ReceiverTasklet>> receiverMap = new HashMap<>();
    // dest vertex id --> dest ordinal --> dest addr --> sender tasklet
    private final Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap = new HashMap<>();

    private List<VertexDef> vertices = new ArrayList<>();
    private final Map<String, ConcurrentConveyor<Object>[]> localConveyorMap = new HashMap<>();
    private final Map<String, Map<Address, ConcurrentConveyor<Object>>> edgeSenderConveyorMap = new HashMap<>();
    private PartitionArrangement ptionArrgmt;

    private NodeEngine nodeEngine;
    private String jetEngineName;
    private long executionId;


    public ExecutionPlan() {
    }

    public List<VertexDef> getVertices() {
        return vertices;
    }

    public void addVertex(VertexDef vertex) {
        vertices.add(vertex);
    }

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
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        vertices = readList(in);
    }

    public void initialize(NodeEngine nodeEngine, String jetEngineName, long executionId) {
        this.nodeEngine = nodeEngine;
        this.jetEngineName = jetEngineName;
        this.executionId = executionId;
        this.ptionArrgmt = new PartitionArrangement(nodeEngine);
        initDag();

        for (VertexDef srcVertex : getVertices()) {
            int processorIdx = -1;
            for (Processor p : createProcessors(srcVertex, srcVertex.parallelism())) {
                processorIdx++;
                // createOutboundEdgeStreams() populates localConveyorMap and edgeSenderConveyorMap.
                // Also populates instance fields: senderMap, receiverMap, tasklets.
                final List<OutboundEdgeStream> outboundStreams = createOutboundEdgeStreams(srcVertex, processorIdx);
                final List<InboundEdgeStream> inboundStreams = createInboundEdgeStreams(srcVertex, processorIdx);
                tasklets.add(new ProcessorTasklet(p, inboundStreams, outboundStreams));
            }
        }
        tasklets.addAll(receiverMap.values().stream().map(Map::values).flatMap(Collection::stream).collect(toList()));
    }

    public List<ProcessorSupplier> getProcSuppliers() {
        return procSuppliers;
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

    /**
     * Initializes partitioners on edges and processor suppliers on vertices. Populates
     * the transient fields on edges. Populates {@code procSuppliers} with all processor
     * suppliers found in the plan.
     */
    private void initDag() {
        final Map<Integer, VertexDef> vMap = getVertices().stream().collect(toMap(VertexDef::vertexId, v -> v));
        getVertices().stream().forEach(v -> {
            v.inputs().forEach(e -> e.initTransientFields(vMap, v, false));
            v.outputs().forEach(e -> e.initTransientFields(vMap, v, true));
        });
        final IPartitionService partitionService = nodeEngine.getPartitionService();
        getVertices().stream()
                     .map(VertexDef::outputs)
                     .flatMap(List::stream)
                     .map(EdgeDef::partitioner)
                     .filter(Objects::nonNull)
                     .forEach(p -> p.init(partitionService::getPartitionId));
        procSuppliers.addAll(getVertices()
                .stream()
                .peek(v -> {
                    final ProcessorSupplier sup = v.processorSupplier();
                    final int parallelism = v.parallelism();
                    sup.init(new ProcSupplierContext(nodeEngine.getHazelcastInstance(), parallelism));
                })
                .map(VertexDef::processorSupplier)
                .collect(toList()));
    }

    private static List<Processor> createProcessors(VertexDef vertexDef, int parallelism) {
        final List<Processor> processors = vertexDef.processorSupplier().get(parallelism);
        if (processors.size() != parallelism) {
            throw new HazelcastException("ProcessorSupplier failed to return the requested number of processors." +
                    " Requested: " + parallelism + ", returned: " + processors.size());
        }
        return processors;
    }

    /**
     * NOTE: populates {@code localConveyorMap}, {@code edgeSenderConveyorMap}.
     * Populates {@link #senderMap} and {@link #tasklets} fields.
     */
    private List<OutboundEdgeStream> createOutboundEdgeStreams(VertexDef srcVertex, int processorIdx) {
        final List<OutboundEdgeStream> outboundStreams = new ArrayList<>();
        for (EdgeDef edge : srcVertex.outputs()) {
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
     * Updates the {@link #senderMap} and {@link #tasklets} fields.
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
                final SenderTasklet t = new SenderTasklet(inboundEdgeStream, nodeEngine, jetEngineName,
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
        return new OutboundEdgeStream(edge.sourceOrdinal(), edge.getConfig().getHighWaterMark(),
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
                       //TODO: fix FLOW_CONTROL_PERIOD after JetConfig is integrated
                       return new ReceiverTasklet(collector, edge.getConfig().getReceiveWindowMultiplier(),
                               JetService.FLOW_CONTROL_PERIOD_MS, senderCount);
                   });
    }

    private List<InboundEdgeStream> createInboundEdgeStreams(VertexDef srcVertex, int processorIdx) {
        final List<InboundEdgeStream> inboundStreams = new ArrayList<>();
        for (EdgeDef inEdge : srcVertex.inputs()) {
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

