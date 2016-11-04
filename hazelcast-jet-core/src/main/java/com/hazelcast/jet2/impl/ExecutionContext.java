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

package com.hazelcast.jet2.impl;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.Member;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorMetaSupplier;
import com.hazelcast.jet2.ProcessorSupplier;
import com.hazelcast.jet2.ProcessorSupplier.Context;
import com.hazelcast.jet2.Vertex;
import com.hazelcast.jet2.impl.deployment.DeploymentStore;
import com.hazelcast.jet2.impl.deployment.JetClassLoader;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.SimpleExecutionCallback;
import com.hazelcast.spi.partition.IPartitionService;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.concurrentConveyor;
import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;
import static com.hazelcast.jet2.impl.OutboundCollector.compositeCollector;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class ExecutionContext {

    private static final int DEFAULT_RESOURCE_CHUNK_SIZE = 1 << 14;
    private static final int QUEUE_SIZE = 1024;

    private final String name;
    private final IdGenerator idGenerator;
    private NodeEngine nodeEngine;
    private ExecutionService executionService;
    private DeploymentStore deploymentStore;
    private JetEngineConfig config;

    // execution id -> vertex id -> ordinal -> receiver
    private Map<Long, Map<Integer, Map<Integer, ReceiverTasklet>>> receiverMap = new ConcurrentHashMap<>();
    // execution id to tasklet list
    private Map<Long, List<Tasklet>> tasklets = new ConcurrentHashMap<>();

    public ExecutionContext(String name, NodeEngine nodeEngine, JetEngineConfig config) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.config = config;
        this.idGenerator = nodeEngine.getHazelcastInstance().getIdGenerator("__jetIdGenerator" + name);
        this.deploymentStore = new DeploymentStore(config.getDeploymentDirectory(), DEFAULT_RESOURCE_CHUNK_SIZE);
        final ClassLoader cl = AccessController.doPrivileged(
                (PrivilegedAction<ClassLoader>) () -> new JetClassLoader(deploymentStore));
        this.executionService = new ExecutionService(nodeEngine.getHazelcastInstance(), name, config, cl);
    }

    public Map<Member, ExecutionPlan> buildExecutionPlan(DAG dag) {
        List<Member> members = new ArrayList<>(nodeEngine.getClusterService().getMembers());
        int clusterSize = members.size();
        final long planId = idGenerator.newId();
        Map<Member, ExecutionPlan> plans = members.stream().collect(toMap(m -> m, m -> new ExecutionPlan(planId)));
        Map<Vertex, Integer> vertexIdMap = assignVertexIds(dag);
        for (Map.Entry<Vertex, Integer> entry : vertexIdMap.entrySet()) {
            Vertex vertex = entry.getKey();
            int vertexId = entry.getValue();
            int perNodeParallelism = getParallelism(vertex);
            int totalParallelism = perNodeParallelism * clusterSize;

            List<Edge> outboundEdges = dag.getOutboundEdges(vertex);
            List<Edge> inboundEdges = dag.getInboundEdges(vertex);

            ProcessorMetaSupplier supplier = vertex.getSupplier();
            supplier.init(ProcessorMetaSupplier.Context.of(
                    nodeEngine.getHazelcastInstance(), totalParallelism, perNodeParallelism));
            List<EdgeDef> outputs = outboundEdges.stream().map(edge -> {
                int otherEndId = vertexIdMap.get(edge.getDestination());
                return new EdgeDef(otherEndId, edge.getOutputOrdinal(), edge.getInputOrdinal(),
                        edge.getPriority(), isDistributed(edge), edge.getForwardingPattern(), edge.getPartitioner());
            }).collect(toList());

            List<EdgeDef> inputs = inboundEdges.stream().map(edge -> {
                int otherEndId = vertexIdMap.get(edge.getSource());
                return new EdgeDef(otherEndId, edge.getInputOrdinal(), edge.getInputOrdinal(),
                        edge.getPriority(), isDistributed(edge), edge.getForwardingPattern(), edge.getPartitioner());
            }).collect(toList());

            for (Entry<Member, ExecutionPlan> e : plans.entrySet()) {
                ProcessorSupplier processorSupplier = supplier.get(e.getKey().getAddress());
                VertexDef vertexDef = new VertexDef(vertexId, processorSupplier, perNodeParallelism);
                vertexDef.addOutputs(outputs);
                vertexDef.addInputs(inputs);
                e.getValue().addVertex(vertexDef);
            }
        }

        return plans;
    }

    public ICompletableFuture<Void> executePlan(long planId) {
        Future<Void> future = executionService.execute(tasklets.get(planId));
        ICompletableFuture<Void> completable = nodeEngine.getExecutionService().asCompletableFuture(future);
        completable.andThen(new SimpleExecutionCallback<Void>() {
            @Override
            public void notify(Object response) {
                tasklets.remove(planId);
            }
        });
        return completable;
    }

    private int getParallelism(Vertex vertex) {
        return vertex.getParallelism() != -1 ? vertex.getParallelism() : config.getParallelism();
    }

    private boolean isDistributed(Edge edge) {
        return edge.isDistributed() && nodeEngine.getClusterService().getSize() > 1;
    }

    private static Map<Vertex, Integer> assignVertexIds(DAG dag) {
        int vertexId = 0;
        Map<Vertex, Integer> vertexIdMap = new LinkedHashMap<>();
        for (Iterator<Vertex> iterator = dag.iterator(); iterator.hasNext(); vertexId++) {
            vertexIdMap.put(iterator.next(), vertexId);
        }
        return vertexIdMap;
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

    public DeploymentStore getDeploymentStore() {
        return deploymentStore;
    }

    public JetEngineConfig getConfig() {
        return config;
    }

    public void handleIncoming(long executionId, int vertexId, int ordinal, int partitionId,
                               byte[] buffer, int offset) {
        Map<Integer, Map<Integer, ReceiverTasklet>> vertexMap = receiverMap.get(executionId);
        if (vertexMap == null) {
            throw new IllegalArgumentException("Execution id " + executionId + " could not be found");
        }
        Map<Integer, ReceiverTasklet> ordinalMap = vertexMap.get(vertexId);
        ReceiverTasklet tasklet = ordinalMap.get(ordinal);
        byte[] data = new byte[buffer.length - offset];
        //TODO: modify HeapData to work with a byte[] and offset
        System.arraycopy(buffer, offset, data, 0, data.length);
        tasklet.offer(new HeapData(data), partitionId);
    }

    public void destroy() {
        deploymentStore.destroy();
        executionService.shutdown();
    }

    public void initializePlan(ExecutionPlan plan) {
        final List<Tasklet> tasks = new ArrayList<>();
        final Map<String, ConcurrentConveyor<Object>[]> conveyorMap = new HashMap<>();
        final Map<Integer, VertexDef> vMap = plan.getVertices().stream().collect(toMap(VertexDef::getId, v -> v));
        // vertex id -> ordinal -> tasklet
        final Map<Integer, Map<Integer, ReceiverTasklet>> receiverTasklets = new HashMap<>();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (VertexDef vertexDef : plan.getVertices()) {
            final List<EdgeDef> inputs = vertexDef.getInputs();
            final List<EdgeDef> outputs = vertexDef.getOutputs();
            final ProcessorSupplier processorSupplier = vertexDef.getProcessorSupplier();
            final int parallelism = vertexDef.getParallelism();
            processorSupplier.init(Context.of(nodeEngine.getHazelcastInstance(), parallelism));
            final List<Processor> processors = processorSupplier.get(parallelism);
            int i = 0;
            for (Processor p : processors) {
                final List<InboundEdgeStream> inboundStreams = new ArrayList<>();
                final List<OutboundEdgeStream> outboundStreams = new ArrayList<>();
                // final copy of i, as needed in lambdas below
                final int taskletIndex = i++;
                for (EdgeDef output : outputs) {
                    final int destinationId = output.getOtherEndId();
                    final int ordinalAtDestination = output.getOtherEndOrdinal();
                    final int localConsumerCount = vMap.get(destinationId).getParallelism();
                    final boolean isDistributed = output.isDistributed();
                    final IPartitionService ptionService = nodeEngine.getPartitionService();
                    final Address thisAddress = nodeEngine.getThisAddress();
                    if (output.getPartitioner() != null) {
                        output.getPartitioner().init(ptionService);
                    }
                    // if a local edge, we will take all partitions, if not only partitions local to this node
                    // and distribute them among the local consumers
                    final Map<Integer, int[]> localPartitions = consumerToPartitions(
                            localConsumerCount, partitionCount, isDistributed, ptionService, thisAddress);

                    // each edge has an array of conveyors
                    // one conveyor per consumer - each conveyor has one queue per producer
                    // giving a total of number of producers * number of consumers queues
                    final String id = vertexDef.getId() + ":" + output.getOtherEndId();
                    final int receiverCount = output.isDistributed() ? 1 : 0;
                    final ConcurrentConveyor<Object>[] conveyorArray = conveyorMap.computeIfAbsent(id,
                            e -> createConveyorArray(localConsumerCount, parallelism + receiverCount, QUEUE_SIZE));
                    final OutboundCollector[] localCollectors = new OutboundCollector[localConsumerCount];
                    Arrays.setAll(localCollectors, n ->
                            new ConveyorCollector(conveyorArray[n], taskletIndex, localPartitions.get(n)));

                    final OutboundCollector[] allCollectors;
                    if (!output.isDistributed()) {
                        allCollectors = localCollectors;
                    } else {
                        // create the receiver tasklet for the edge, if not already created
                        receiverTasklets.computeIfAbsent(destinationId, x -> new HashMap<>());
                        receiverTasklets.get(destinationId).computeIfAbsent(ordinalAtDestination, x -> {
                            final OutboundCollector[] receivers = new OutboundCollector[localConsumerCount];
                            Arrays.setAll(receivers, n ->
                                    new ConveyorCollector(conveyorArray[n], parallelism, localPartitions.get(n)));
                            final OutboundCollector collector = compositeCollector(receivers, output, partitionCount);
                            final int senderCount = nodeEngine.getClusterService().getSize() - 1;
                            return new ReceiverTasklet(
                                    nodeEngine.getSerializationService(), collector, parallelism * senderCount);
                        });
                        // distribute remote partitions
                        final Map<Address, int[]> remotePartitions =
                                addrToPartitions(thisAddress, ptionService.getMemberPartitionsMap());
                        allCollectors = new OutboundCollector[remotePartitions.size() + 1];
                        allCollectors[0] = compositeCollector(localCollectors, output, partitionCount);
                        int index = 1;
                        for (Entry<Address, int[]> entry : remotePartitions.entrySet()) {
                            allCollectors[index++] = new RemoteOutboundCollector(
                                    nodeEngine, name, entry.getKey(), plan.getId(), destinationId,
                                    ordinalAtDestination, entry.getValue());
                        }
                    }
                    outboundStreams.add(new OutboundEdgeStream(
                            output.getOrdinal(), compositeCollector(allCollectors, output, partitionCount)));
                }
                for (EdgeDef input : inputs) {
                    // each tasklet will have one input conveyor per edge
                    // and one InboundEmitter per queue on the conveyor
                    final String id = input.getOtherEndId() + ":" + vertexDef.getId();
                    final ConcurrentConveyor<Object> conveyor = conveyorMap.get(id)[taskletIndex];
                    final InboundEmitter[] emitters = new InboundEmitter[conveyor.queueCount()];
                    Arrays.setAll(emitters, n -> new ConveyorEmitter(conveyor, n));
                    final ConcurrentInboundEdgeStream inboundStream =
                            new ConcurrentInboundEdgeStream(emitters, input.getOrdinal(), input.getPriority());
                    inboundStreams.add(inboundStream);
                }
                tasks.add(new ProcessorTasklet(p, inboundStreams, outboundStreams));
            }
        }

        final List<Tasklet> receivers = receiverTasklets
                .values().stream()
                .flatMap(e -> e.values().stream()).collect(toList());
        tasks.addAll(receivers);
        tasklets.put(plan.getId(), tasks);
        receiverMap.put(plan.getId(), receiverTasklets);
    }

    private static Map<Address, int[]> addrToPartitions(
            Address thisAddress, Map<Address, List<Integer>> partitionOwnerMap
    ) {
        final Map<Address, List<Integer>> addrToPartitions = partitionOwnerMap
                .entrySet().stream()
                .filter(e -> !e.getKey().equals(thisAddress))
                .collect(toMap(Entry::getKey, Entry::getValue));
        return toIntArrayMap(addrToPartitions);
    }

    private static Map<Integer, int[]> consumerToPartitions(
            int localConsumerCount, int partitionCount, boolean isEdgeDistributed,
            IPartitionService ptionService, Address thisAddress
    ) {
        final Map<Integer, List<Integer>> consumerToPartitions = IntStream
                .range(0, partitionCount).boxed()
                .filter(p -> !isEdgeDistributed || ptionService.getPartitionOwner(p).equals(thisAddress))
                .collect(groupingBy(p -> p % localConsumerCount));
        return toIntArrayMap(consumerToPartitions);
    }

    private static <K> Map<K, int[]> toIntArrayMap(Map<K, List<Integer>> intListMap) {
        return intListMap.entrySet().stream()
                .collect(toMap(Entry::getKey, e -> e.getValue().stream().mapToInt(x -> x).toArray()));
    }
}
