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
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.concurrent.QueuedPipe;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.JetEngine;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Job;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorSupplier;
import com.hazelcast.jet2.Vertex;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.concurrent.ConcurrentConveyor.concurrentConveyor;
import static com.hazelcast.jet.impl.util.JetUtil.unchecked;
import static com.hazelcast.jet2.impl.ConcurrentOutboundEdgeStream.newStream;
import static com.hazelcast.jet2.impl.DoneItem.DONE_ITEM;

public class JetEngineImpl extends AbstractDistributedObject<JetService> implements JetEngine {

    public static final int QUEUE_SIZE = 1024;
    public static final int PARALLELISM = 4;
    private final String name;
    private final ILogger logger;
    private final ExecutionService executionService;
    private final JetEngineConfig config;

    protected JetEngineImpl(String name, NodeEngine nodeEngine, JetService service) {
        super(nodeEngine, service);
        this.name = name;
        this.logger = nodeEngine.getLogger(JetEngine.class);
        this.config = new JetEngineConfig()
                .setParallelism(PARALLELISM);
        this.executionService = new ExecutionService(config);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return JetService.SERVICE_NAME;
    }

    @Override
    public Job newJob(DAG dag) {
        return new JobImpl(this, dag);
    }

    public void execute(JobImpl job) {
        executeOperation(new ExecuteJobOperation(getName(), job.getDag()));
    }

    public Future<Void> executeLocal(DAG dag) {
        Map<Edge, ConcurrentConveyor<Object>[]> conveyorMap = new HashMap<>();
        List<Tasklet> tasks = new ArrayList<>();

        for (Vertex vertex : dag) {
            List<Edge> outboundEdges = dag.getOutboundEdges(vertex);
            List<Edge> inboundEdges = dag.getInboundEdges(vertex);
            int parallelism = getParallelism(vertex);
            ProcessorContextImpl context = new ProcessorContextImpl(getNodeEngine().getHazelcastInstance()
                    , parallelism);
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
                    outboundStreams.add(newStream(conveyorArray, outboundEdge, taskletIndex));
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

    private <T> List<T> executeOperation(Operation operation) {
        ClusterService clusterService = getNodeEngine().getClusterService();

        List<ICompletableFuture<T>> futures = new ArrayList<>();
        for (Member member : clusterService.getMembers()) {
            InternalCompletableFuture<T> future = getOperationService()
                    .createInvocationBuilder(JetService.SERVICE_NAME, operation, member.getAddress())
                    .<T>invoke();
            futures.add(future);
        }
        try {
            List<T> results = new ArrayList<>();
            for (ICompletableFuture<T> future : futures) {
                results.add(future.get());
            }
            return results;
        } catch (InterruptedException | ExecutionException e) {
            throw unchecked(e);
        }
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


}

