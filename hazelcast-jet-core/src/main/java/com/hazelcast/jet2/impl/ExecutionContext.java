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

import com.hazelcast.core.Member;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorListSupplier;
import com.hazelcast.jet2.ProcessorMetaSupplier;
import com.hazelcast.jet2.Vertex;
import com.hazelcast.jet2.impl.deployment.DeploymentStore;
import com.hazelcast.jet2.impl.deployment.JetClassLoader;
import com.hazelcast.spi.NodeEngine;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.concurrentConveyor;
import static com.hazelcast.jet2.impl.ConcurrentOutboundEdgeStream.newStream;
import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;

public class ExecutionContext {

    public static final int QUEUE_SIZE = 1024;

    private NodeEngine nodeEngine;
    private ExecutionService executionService;
    private DeploymentStore deploymentStore;
    private JetEngineConfig config;
    private JetClassLoader classLoader;

    public ExecutionContext(NodeEngine nodeEngine, ExecutionService executionService, DeploymentStore deploymentStore,
                            JetEngineConfig config) {
        this.nodeEngine = nodeEngine;
        this.executionService = executionService;
        this.deploymentStore = deploymentStore;
        this.config = config;
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            this.classLoader = new JetClassLoader(deploymentStore);
            return null;
        });
    }

    public Map<Member, ExecutionPlan> buildExecutionPlan(DAG dag) {
        List<Member> members = new ArrayList<>(nodeEngine.getClusterService().getMembers());
        int clusterSize = members.size();
        Map<Member, ExecutionPlan> plans = members.stream().collect(Collectors.toMap(m -> m, m -> new ExecutionPlan()));
        Map<Vertex, Integer> vertexIdMap = assignVertexIds(dag);
        for (Map.Entry<Vertex, Integer> entry : vertexIdMap.entrySet()) {
            Vertex vertex = entry.getKey();
            int vertexId = entry.getValue();
            int perNodeParallelism = getParallelism(vertex);
            int totalParallelism = perNodeParallelism * clusterSize;

            List<Edge> outboundEdges = dag.getOutboundEdges(vertex);
            List<Edge> inboundEdges = dag.getInboundEdges(vertex);

            MetaProcessorSupplierContextImpl context = new MetaProcessorSupplierContextImpl(
                    nodeEngine.getHazelcastInstance(), totalParallelism, perNodeParallelism);
            ProcessorMetaSupplier supplier = vertex.getSupplier();
            supplier.init(context);

            List<EdgeDef> outputs = new ArrayList<>();
            for (Edge edge : outboundEdges) {
                int otherEndId = vertexIdMap.get(edge.getDestination());
                int otherEndParallelism = getParallelism(edge.getDestination());
                EdgeDef edgeDef = new EdgeDef(vertexId + ":" + otherEndId,
                        otherEndId, otherEndParallelism, edge.getOutputOrdinal(),
                        edge.getPriority(), edge.getForwardingPattern(), edge.getPartitioner());
                outputs.add(edgeDef);
            }
            List<EdgeDef> inputs = new ArrayList<>();
            for (Edge edge : inboundEdges) {
                int otherEndId = vertexIdMap.get(edge.getSource());
                int otherEndParallelism = getParallelism(edge.getSource());
                EdgeDef edgeDef = new EdgeDef(otherEndId + ":" + vertexId,
                        otherEndId, otherEndParallelism, edge.getInputOrdinal(),
                        edge.getPriority(), edge.getForwardingPattern(), edge.getPartitioner());
                inputs.add(edgeDef);
            }

            for (Member member : members) {
                ExecutionPlan plan = plans.get(member);
                ProcessorListSupplier processorListSupplier = supplier.get(member.getAddress());
                VertexDef vertexDef = new VertexDef(vertexId, processorListSupplier, perNodeParallelism);
                vertexDef.addOutputs(outputs);
                vertexDef.addInputs(inputs);
                plan.addVertex(vertexDef);
            }
        }

        return plans;

    }

    private Map<Vertex, Integer> assignVertexIds(DAG dag) {
        int vertexId = 0;
        Map<Vertex, Integer> vertexIdMap = new LinkedHashMap<>();
        for (Iterator<Vertex> iterator = dag.iterator(); iterator.hasNext(); vertexId++) {
            vertexIdMap.put(iterator.next(), vertexId);
        }
        return vertexIdMap;
    }

    public Future<Void> executePlan(ExecutionPlan plan) {
        List<Tasklet> tasks = new ArrayList<>();
        Map<String, ConcurrentConveyor<Object>[]> conveyorMap = new HashMap<>();

        for (VertexDef vertexDef : plan.getVertices()) {
            List<EdgeDef> inputs = vertexDef.getInputs();
            List<EdgeDef> outputs = vertexDef.getOutputs();
            ProcessorListSupplier processorSupplier = vertexDef.getProcessorSupplier();
            int parallelism = vertexDef.getParallelism();
            processorSupplier.init(new ProcessorSupplierContextImpl(nodeEngine.getHazelcastInstance(), parallelism));
            List<Processor> processors = processorSupplier.get(parallelism);
            for (int i = 0; i < parallelism; i++) {
                List<InboundEdgeStream> inboundStreams = new ArrayList<>();
                List<OutboundEdgeStream> outboundStreams = new ArrayList<>();
                final int taskletIndex = i; // taskletIndex is final, unlike i
                for (EdgeDef output : outputs) {
                    // each edge has an array of conveyors
                    // one conveyor per consumer - each conveyor has one queue per producer
                    // giving a total of number of producers * number of consumers queues
                    final ConcurrentConveyor<Object>[] conveyorArray = conveyorMap.computeIfAbsent(output.getId(), e ->
                            createConveyorArray(output.getOtherEndParallelism(), parallelism, QUEUE_SIZE));
                    OutboundCollector[] collectors = new OutboundCollector[conveyorArray.length];
                    Arrays.setAll(collectors, n -> new ConveyorCollector(conveyorArray[n], taskletIndex));
                    outboundStreams.add(newStream(collectors, output));
                }
                for (EdgeDef input : inputs) {
                    // each tasklet will have one input conveyor per edge
                    // and one InboundEmitter per queue on the conveyor
                    final ConcurrentConveyor<Object> conveyor = conveyorMap.get(input.getId())[taskletIndex];
                    InboundEmitter[] emitters = new InboundEmitter[conveyor.queueCount()];
                    Arrays.setAll(emitters, n -> new ConveyorEmitter(conveyor, n));
                    ConcurrentInboundEdgeStream inboundStream = new ConcurrentInboundEdgeStream(
                            emitters, input.getOrdinal(), input.getPriority());
                    inboundStreams.add(inboundStream);
                }
                tasks.add(new ProcessorTasklet(processors.get(i), classLoader, inboundStreams, outboundStreams));
            }
        }
        return executionService.execute(tasks);
    }

//    public Future<Void> execute(DAG dag) {
//        Map<Edge, ConcurrentConveyor<Object>[]> conveyorMap = new HashMap<>();
//        List<Tasklet> tasks = new ArrayList<>();
//
//        for (Vertex vertex : dag) {
//            List<Edge> outboundEdges = dag.getOutboundEdges(vertex);
//            List<Edge> inboundEdges = dag.getInboundEdges(vertex);
//            int parallelism = getParallelism(vertex);
//            int totalParallelism = nodeEngine.getClusterService().getSize() * parallelism;
//            MetaProcessorSupplier metaSupplier = vertex.getSupplier();
//            metaSupplier.init(new MetaProcessorSupplierContextImpl(nodeEngine.getHazelcastInstance(),
//                    totalParallelism, parallelism));
//
//            ProcessorListSupplier procSupplier = metaSupplier.get(nodeEngine.getThisAddress());
//            procSupplier.init(new ProcessorSupplierContextImpl(nodeEngine.getHazelcastInstance(), parallelism));
//            for (int taskletIndex = 0; taskletIndex < parallelism; taskletIndex++) {
//                List<OutboundEdgeStream> outboundStreams = new ArrayList<>();
//                List<InboundEdgeStream> inboundStreams = new ArrayList<>();
//                for (Edge outboundEdge : outboundEdges) {
//                    // each edge has an array of conveyors
//                    // one conveyor per consumer - each conveyor has one queue per producer
//                    // giving a total of number of producers * number of consumers queues
//                    ConcurrentConveyor<Object>[] conveyorArray = conveyorMap.computeIfAbsent(outboundEdge, e ->
//                            createConveyorArray(getParallelism(outboundEdge.getDestination()), parallelism, QUEUE_SIZE));
//                    OutboundCollector[] collectors = new OutboundCollector[conveyorArray.length];
//                    int ti = taskletIndex; // ti is effectively final, unlike taskletIndex
//                    Arrays.setAll(collectors, i -> new ConveyorCollector(conveyorArray[i], ti));
//                    outboundStreams.add(newStream(collectors, outboundEdge));
//                }
//
//                for (Edge inboundEdge : inboundEdges) {
//                    ConcurrentConveyor<Object> conveyor = conveyorMap.get(inboundEdge)[taskletIndex];
//                    InboundEmitter[] emitters = new InboundEmitter[conveyor.queueCount()];
//                    Arrays.setAll(emitters, i -> new ConveyorEmitter(conveyor, i));
//                    ConcurrentInboundEdgeStream inboundStream = new ConcurrentInboundEdgeStream(
//                            emitters, inboundEdge.getInputOrdinal(), inboundEdge.getPriority());
//                    inboundStreams.add(inboundStream);
//                }
//                procSupplier.get(parallelism).stream()
//                        .map(p -> new ProcessorTasklet(p, classLoader, inboundStreams, outboundStreams))
//                        .forEach(tasks::add);
//            }
//        }
//        return executionService.execute(tasks);
//    }

    private int getParallelism(Vertex vertex) {
        return vertex.getParallelism() != -1 ? vertex.getParallelism() : config.getParallelism();
    }

    @SuppressWarnings("unchecked")
    private ConcurrentConveyor<Object>[] createConveyorArray(int count, int queueCount, int queueSize) {
        ConcurrentConveyor<Object>[] concurrentConveyors = new ConcurrentConveyor[count];
        Arrays.setAll(concurrentConveyors, i -> {
            QueuedPipe<Object>[] queues = new QueuedPipe[queueCount];
            Arrays.setAll(queues, j -> new OneToOneConcurrentArrayQueue<>(queueSize));
            return concurrentConveyor(DONE_ITEM, queues);
        });
        return concurrentConveyors;
    }

    // GETTERs

    public ExecutionService getExecutionService() {
        return executionService;
    }

    public DeploymentStore getDeploymentStore() {
        return deploymentStore;
    }

    public JetEngineConfig getConfig() {
        return config;
    }

    public JetClassLoader getClassLoader() {
        return classLoader;
    }

    public void destroy() {
        executionService.shutdown();
    }
}

