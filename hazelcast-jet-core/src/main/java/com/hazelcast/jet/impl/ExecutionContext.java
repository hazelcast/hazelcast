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

package com.hazelcast.jet.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.function.Consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.concurrentConveyor;
import static com.hazelcast.jet.impl.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.OutboundCollector.compositeCollector;
import static com.hazelcast.jet.impl.Util.getRemoteMembers;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

class ExecutionContext {

    // vertex id --> ordinal --> receiver tasklet
    private Map<Integer, Map<Integer, ReceiverTasklet>> receiverMap;

    // dest vertex id --> dest ordinal --> dest addr --> sender tasklet
    private Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap;

    private Map<Address, Integer> memberToId;

    private final long executionId;
    private final NodeEngine nodeEngine;
    private final EngineContext context;
    // populated at init time with all processor suppliers; required during complete()
    private final List<ProcessorSupplier> procSuppliers = new ArrayList<>();
    private final List<Tasklet> tasklets = new ArrayList<>();
    private CompletionStage<Void> executionCompletionStage;

    ExecutionContext(long executionId, EngineContext context) {
        this.executionId = executionId;
        this.context = context;
        this.nodeEngine = context.getNodeEngine();
    }

    CompletionStage<Void> execute(Consumer<CompletionStage<Void>> doneCallback) {
        executionCompletionStage = context.getExecutionService().execute(tasklets, doneCallback);
        executionCompletionStage.whenComplete((r, e) -> tasklets.clear());
        return executionCompletionStage;
    }

    public CompletionStage<Void> getExecutionCompletionStage() {
        return executionCompletionStage;
    }

    Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap() {
        return senderMap;
    }

    Map<Integer, Map<Integer, ReceiverTasklet>> receiverMap() {
        return receiverMap;
    }

    void complete(Throwable error) {
        procSuppliers.forEach(s -> s.complete(error));
    }

    void handlePacket(int vertexId, int ordinal, Address sender, BufferObjectDataInput in) {
        receiverMap.get(vertexId)
                   .get(ordinal)
                   .receiveStreamPacket(in, memberToId.get(sender));
    }

    Integer getMemberId(Address member) {
        return memberToId != null ? memberToId.get(member) : null;
    }

    ExecutionContext initialize(ExecutionPlan plan) {
        procSuppliers.addAll(plan.init(nodeEngine));
        populateMemberToId();
        final PartitionArrangement ptionArrgmt = new PartitionArrangement(nodeEngine);

        receiverMap = new HashMap<>();
        senderMap = new HashMap<>();
        final Map<String, ConcurrentConveyor<Object>[]> localConveyorMap = new HashMap<>();
        final Map<String, Map<Address, ConcurrentConveyor<Object>>> edgeSenderConveyorMap = new HashMap<>();

        for (VertexDef srcVertex : plan.getVertices()) {
            int processorIdx = -1;
            for (Processor p : createProcessors(srcVertex, srcVertex.parallelism())) {
                processorIdx++;
                // createOutboundEdgeStreams() populates localConveyorMap and edgeSenderConveyorMap.
                // Also populates instance fields: senderMap, receiverMap, tasklets.
                final List<OutboundEdgeStream> outboundStreams = createOutboundEdgeStreams(
                        localConveyorMap, edgeSenderConveyorMap, ptionArrgmt, srcVertex, processorIdx);
                final List<InboundEdgeStream> inboundStreams = createInboundEdgeStreams(
                        localConveyorMap, srcVertex, processorIdx);
                tasklets.add(new ProcessorTasklet(p, inboundStreams, outboundStreams));
            }
        }
        receiverMap = unmodifiableMap(receiverMap);
        senderMap = unmodifiableMap(senderMap);
        tasklets.addAll(receiverMap.values().stream().map(Map::values).flatMap(Collection::stream).collect(toList()));
        return this;
    }

    private int totalPartitionCount() {
        return nodeEngine.getPartitionService().getPartitionCount();
    }

    private void populateMemberToId() {
        final Map<Address, Integer> memberToId = new HashMap<>();
        int id = 0;
        for (Address member : getRemoteMembers(nodeEngine)) {
            memberToId.put(member, id++);
        }
        this.memberToId = unmodifiableMap(memberToId);
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
    private List<OutboundEdgeStream> createOutboundEdgeStreams(
            Map<String, ConcurrentConveyor<Object>[]> localConveyorMap,
            Map<String, Map<Address, ConcurrentConveyor<Object>>> edgeSenderConveyorMap,
            PartitionArrangement ptionArrgmt, VertexDef srcVertex, int processorIdx
    ) {
        final List<OutboundEdgeStream> outboundStreams = new ArrayList<>();
        for (EdgeDef edge : srcVertex.outputs()) {
            // each edge has an array of conveyors
            // one conveyor per consumer - each conveyor has one queue per producer
            // giving the total number of queues = producers * consumers
            final ConcurrentConveyor<Object>[] localConveyors = localConveyorMap.computeIfAbsent(edge.edgeId(),
                    e -> createConveyorArray(edge.destVertex().parallelism(),
                            srcVertex.parallelism() + (edge.isDistributed() ? 1 : 0),
                            edge.getConfig().getQueueSize()));
            final Map<Address, ConcurrentConveyor<Object>> memberToSenderConveyorMap =
                    edge.isDistributed() ? memberToSenderConveyorMap(edgeSenderConveyorMap, edge) : null;
            outboundStreams.add(createOutboundEdgeStream(
                    edge, processorIdx, localConveyors, memberToSenderConveyorMap, ptionArrgmt));
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
                final SenderTasklet t = new SenderTasklet(inboundEdgeStream, nodeEngine, context.getName(),
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
            EdgeDef edge, int processorIndex, ConcurrentConveyor<Object>[] localConveyors,
            Map<Address, ConcurrentConveyor<Object>> senderConveyorMap, PartitionArrangement ptionArrgmt
    ) {
        final int totalPtionCount = totalPartitionCount();
        final int processorCount = edge.destVertex().parallelism();
        final int[][] ptionsPerProcessor = ptionArrgmt.assignPartitionsToProcessors(processorCount, edge.isDistributed());
        final OutboundCollector[] localCollectors = new OutboundCollector[processorCount];
        Arrays.setAll(localCollectors, n ->
                new ConveyorCollector(localConveyors[n], processorIndex, ptionsPerProcessor[n]));

        final OutboundCollector[] allCollectors;
        if (!edge.isDistributed()) {
            allCollectors = localCollectors;
        } else {
            createIfAbsentReceiverTasklet(edge, localConveyors, ptionsPerProcessor, totalPtionCount);
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

    private void createIfAbsentReceiverTasklet(
            EdgeDef edge, ConcurrentConveyor<Object>[] localConveyors, int[][] ptionsPerProcessor, int totalPtionCount
    ) {
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

    private static List<InboundEdgeStream> createInboundEdgeStreams(
            Map<String, ConcurrentConveyor<Object>[]> localConveyorMap, VertexDef srcVertex, int processorIdx
    ) {
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
