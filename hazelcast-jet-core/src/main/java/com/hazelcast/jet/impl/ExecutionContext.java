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
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.function.Consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private final List<ProcessorSupplier> suppliers = new ArrayList<>();
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
        suppliers.forEach(s -> s.complete(error));
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
        // make a copy of all suppliers - required for complete() call
        suppliers.addAll(plan.getVertices().stream().map(VertexDef::getProcessorSupplier).collect(toList()));
        receiverMap = new HashMap<>();
        senderMap = new HashMap<>();
        final PartitionArrangement ptionArrgmt = new PartitionArrangement(nodeEngine);
        final Map<String, ConcurrentConveyor<Object>[]> localConveyorMap = new HashMap<>();
        final Map<String, Map<Address, ConcurrentConveyor<Object>>> senderConveyorMap = new HashMap<>();
        final Map<Integer, VertexDef> vMap = plan.getVertices().stream().collect(toMap(VertexDef::getVertexId, v -> v));
        final List<Address> remoteMembers = getRemoteMembers(nodeEngine);
        populateMemberToId(remoteMembers);
        for (VertexDef vertex : plan.getVertices()) {
            final List<EdgeDef> inputs = vertex.getInputs();
            final List<EdgeDef> outputs = vertex.getOutputs();
            final int parallelism = vertex.getParallelism();
            final List<Processor> processors = initVertex(vertex);
            for (int processorIdx = 0; processorIdx < processors.size(); processorIdx++) {
                final Processor p = processors.get(processorIdx);
                final List<InboundEdgeStream> inboundStreams = new ArrayList<>();
                final List<OutboundEdgeStream> outboundStreams = new ArrayList<>();
                for (EdgeDef edge : outputs) {
                    final int destVertexId = edge.getOppositeVertexId();
                    final String edgeId = vertex.getVertexId() + ":" + edge.getOppositeVertexId();
                    final VertexDef destination = vMap.get(destVertexId);
                    final int localProcessorCount = destination.getParallelism();
                    final int receiverCount = edge.isDistributed() ? 1 : 0;

                    // each edge has an array of conveyors
                    // one conveyor per consumer - each conveyor has one queue per producer
                    // giving the total number of queues = producers * consumers
                    final ConcurrentConveyor<Object>[] localConveyors = localConveyorMap.computeIfAbsent(edgeId,
                            e -> createConveyorArray(localProcessorCount, parallelism + receiverCount,
                                    edge.getConfig().getQueueSize()));

                    // create a sender tasklet per destination address, each with a single conveyor with number of
                    // producers queues feeding it
                    final Map<Address, ConcurrentConveyor<Object>> senderConveyor = !edge.isDistributed() ? null :
                            senderConveyorMap.computeIfAbsent(edgeId, k -> {
                                final Map<Address, ConcurrentConveyor<Object>> addrToConveyor = new HashMap<>();
                                for (Address destAddr : remoteMembers) {
                                    final ConcurrentConveyor<Object> conveyor =
                                            createConveyorArray(1, parallelism, edge.getConfig().getQueueSize())[0];
                                    final ConcurrentInboundEdgeStream inboundEdgeStream = createInboundEdgeStream(
                                            edge.getOppositeEndOrdinal(), edge.getPriority(), conveyor);
                                    final SenderTasklet t = new SenderTasklet(inboundEdgeStream, nodeEngine,
                                            context.getName(), destAddr, executionId, destVertexId,
                                            edge.getConfig().getPacketSizeLimit());
                                    senderMap.computeIfAbsent(destVertexId, x -> new HashMap<>())
                                             .computeIfAbsent(edge.getOppositeEndOrdinal(), x -> new HashMap<>())
                                             .put(destAddr, t);
                                    tasklets.add(t);
                                    addrToConveyor.put(destAddr, conveyor);
                                }
                                return addrToConveyor;
                            });
                    outboundStreams.add(createOutboundEdgeStream(
                            vertex, destination, edge, processorIdx, localConveyors, senderConveyor, ptionArrgmt));
                }
                for (EdgeDef input : inputs) {
                    // each tasklet will have one input conveyor per edge
                    // and one InboundEmitter per queue on the conveyor
                    final String id = input.getOppositeVertexId() + ":" + vertex.getVertexId();
                    final ConcurrentConveyor<Object> conveyor = localConveyorMap.get(id)[processorIdx];
                    inboundStreams.add(createInboundEdgeStream(input.getOrdinal(), input.getPriority(), conveyor));
                }
                tasklets.add(new ProcessorTasklet(p, inboundStreams, outboundStreams));
            }
        }
        receiverMap = unmodifiableMap(receiverMap);
        senderMap = unmodifiableMap(senderMap);
        tasklets.addAll(receiverMap.values().stream().flatMap(e -> e.values().stream()).collect(toList()));
        return this;
    }

    private int totalPartitionCount() {
        return nodeEngine.getPartitionService().getPartitionCount();
    }

    private void populateMemberToId(List<Address> remoteMembers) {
        final Map<Address, Integer> memberToId = new HashMap<>();
        int id = 0;
        for (Address member : remoteMembers) {
            memberToId.put(member, id++);
        }
        this.memberToId = unmodifiableMap(memberToId);
    }

    private static ConcurrentInboundEdgeStream createInboundEdgeStream(
            int ordinal, int priority, ConcurrentConveyor<Object> conveyor
    ) {
        final InboundEmitter[] emitters = new InboundEmitter[conveyor.queueCount()];
        Arrays.setAll(emitters, n -> new ConveyorEmitter(conveyor, n));
        return new ConcurrentInboundEdgeStream(emitters, ordinal, priority);
    }

    private OutboundEdgeStream createOutboundEdgeStream(
            VertexDef source, VertexDef destination, EdgeDef edge, int taskletIndex,
            ConcurrentConveyor<Object>[] localConveyors,
            Map<Address, ConcurrentConveyor<Object>> senderConveyorMap, PartitionArrangement ptionArrgmt
    ) {
        final int totalPtionCount = totalPartitionCount();
        final int processorCount = destination.getParallelism();
        final int[][] ptionsPerProcessor = ptionArrgmt.assignPartitionsToProcessors(processorCount, edge.isDistributed());
        final int ordinalAtDestination = edge.getOppositeEndOrdinal();
        final OutboundCollector[] localCollectors = new OutboundCollector[processorCount];
        Arrays.setAll(localCollectors, n ->
                new ConveyorCollector(localConveyors[n], taskletIndex, ptionsPerProcessor[n]));

        final int parallelism = source.getParallelism();
        final int destVertexId = edge.getOppositeVertexId();
        final OutboundCollector[] allCollectors;
        if (!edge.isDistributed()) {
            allCollectors = localCollectors;
        } else {
            // create the receiver tasklet for the edge, if not already created
            receiverMap.computeIfAbsent(destVertexId, x -> new HashMap<>())
                       .computeIfAbsent(ordinalAtDestination, x -> {
                           final OutboundCollector[] receivers = new OutboundCollector[processorCount];
                           Arrays.setAll(receivers, n ->
                                   new ConveyorCollector(localConveyors[n], parallelism, ptionsPerProcessor[n]));
                           final OutboundCollector collector = compositeCollector(receivers, edge, totalPtionCount);
                           final int senderCount = nodeEngine.getClusterService().getSize() - 1;
                           //TODO: fix FLOW_CONTROL_PERIOD after JetConfig is integrated
                           return new ReceiverTasklet(collector, edge.getConfig().getReceiveWindowMultiplier(),
                                   JetService.FLOW_CONTROL_PERIOD_MS, senderCount);
                       });
            // assign remote partitions to outbound data collectors
            final Map<Address, int[]> remotePartitions = ptionArrgmt.remotePartitionAssignment.get();
            allCollectors = new OutboundCollector[remotePartitions.size() + 1];
            allCollectors[0] = compositeCollector(localCollectors, edge, totalPtionCount);
            int index = 1;
            for (Map.Entry<Address, int[]> entry : remotePartitions.entrySet()) {
                allCollectors[index++] = new ConveyorCollectorWithPartition(senderConveyorMap.get(entry.getKey()),
                        taskletIndex, entry.getValue());
            }
        }
        return new OutboundEdgeStream(edge.getOrdinal(), edge.getConfig().getHighWaterMark(),
                compositeCollector(allCollectors, edge, totalPtionCount));
    }

    private List<Processor> initVertex(VertexDef vertexDef) {
        final IPartitionService partitionService = nodeEngine.getPartitionService();
        vertexDef.getOutputs().stream()
                 .map(EdgeDef::getPartitioner)
                 .filter(Objects::nonNull)
                 .forEach(p -> p.init(partitionService::getPartitionId));
        final ProcessorSupplier processorSupplier = vertexDef.getProcessorSupplier();
        final int parallelism = vertexDef.getParallelism();
        processorSupplier.init(ProcessorSupplier.Context.of(nodeEngine.getHazelcastInstance(), parallelism));
        final List<Processor> processors = processorSupplier.get(parallelism);
        if (processors.size() != parallelism) {
            throw new HazelcastException("ProcessorSupplier failed to return the requested number of processors." +
                    " Requested: " + parallelism + ", returned: " + processors.size());
        }
        return processors;
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
}
