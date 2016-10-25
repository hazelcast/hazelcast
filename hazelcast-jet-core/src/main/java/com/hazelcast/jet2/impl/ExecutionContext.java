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

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorSupplier;
import com.hazelcast.jet2.Vertex;
import com.hazelcast.jet2.impl.deployment.DeploymentStore;
import com.hazelcast.jet2.impl.deployment.JetClassLoader;
import com.hazelcast.spi.NodeEngine;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

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

    public Future<Void> execute(DAG dag) {
        Map<Edge, ConcurrentConveyor<Object>[]> conveyorMap = new HashMap<>();
        List<Tasklet> tasks = new ArrayList<>();

        for (Vertex vertex : dag) {
            List<Edge> outboundEdges = dag.getOutboundEdges(vertex);
            List<Edge> inboundEdges = dag.getInboundEdges(vertex);
            int parallelism = getParallelism(vertex);
            ProcessorContextImpl context = new ProcessorContextImpl(nodeEngine.getHazelcastInstance(),
                    parallelism, classLoader);
            ProcessorSupplier supplier = vertex.getProcessorSupplier();
            supplier.init(context);
            for (int taskletIndex = 0; taskletIndex < parallelism; taskletIndex++) {
                List<OutboundEdgeStream> outboundStreams = new ArrayList<>();
                List<InboundEdgeStream> inboundStreams = new ArrayList<>();
                for (Edge outboundEdge : outboundEdges) {
                    // each edge has an array of conveyors
                    // one conveyor per consumer - each conveyor has one queue per producer
                    // giving a total of number of producers * number of consumers queues
                    ConcurrentConveyor<Object>[] conveyorArray = conveyorMap.computeIfAbsent(outboundEdge, e ->
                            createConveyorArray(getParallelism(outboundEdge.getDestination()),
                                    parallelism, QUEUE_SIZE));
                    OutboundConsumer[] consumers = new OutboundConsumer[conveyorArray.length];
                    for (int i = 0, conveyorsLength = conveyorArray.length; i < conveyorsLength; i++) {
                        consumers[i] = new ConveyorConsumer(conveyorArray[i], taskletIndex);
                    }
                    outboundStreams.add(newStream(consumers, outboundEdge));
                }

                for (Edge inboundEdge : inboundEdges) {
                    ConcurrentConveyor<Object>[] conveyors = conveyorMap.get(inboundEdge);
                    ConcurrentInboundEdgeStream inboundStream =
                            new ConcurrentInboundEdgeStream(conveyors[taskletIndex],
                                    inboundEdge.getInputOrdinal(),
                                    inboundEdge.getPriority());
                    inboundStreams.add(inboundStream);
                }

                Processor processor = supplier.get();
                tasks.add(new ProcessorTasklet(context, processor, inboundStreams, outboundStreams));
            }
        }

        return executionService.execute(tasks);
    }

    private int getParallelism(Vertex vertex) {
        int parallelism = vertex.getParallelism();
        return parallelism != -1 ? parallelism : config.getParallelism();
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

}

