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

package com.hazelcast.jet.impl.statemachine.jobmanager.processors;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.runtime.DataChannel;
import com.hazelcast.jet.impl.runtime.JobManager;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.runtime.VertexRunnerPayloadProcessor;
import com.hazelcast.jet.impl.statemachine.runner.requests.VertexRunnerStartRequest;
import com.hazelcast.logging.ILogger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class ExecutionPlanBuilderProcessor implements VertexRunnerPayloadProcessor<DAG> {

    private final JobManager jobManager;
    private final JobContext jobContext;
    private final ILogger logger;
    private final AtomicInteger idGenerator = new AtomicInteger();


    public ExecutionPlanBuilderProcessor(JobManager jobManager) {
        this.jobManager = jobManager;
        this.jobContext = jobManager.getJobContext();
        this.logger = jobContext.getNodeEngine().getLogger(getClass());
    }

    @Override
    public void process(DAG dag) throws Exception {
        checkNotNull(dag);
        logger.fine("Processing DAG " + dag.getName());
        //Process dag and vertex runners's chain building
        Iterator<Vertex> iterator = dag.getTopologicalVertexIterator();
        Map<Vertex, VertexRunner> vertex2RunnerMap = new HashMap<>(dag.getVertices().size());
        while (iterator.hasNext()) {
            Vertex vertex = iterator.next();
            logger.fine("Processing vertex=" + vertex.getName() + " for DAG " + dag.getName());
            List<Edge> edges = vertex.getInputEdges();
            VertexRunner next = createRunner(vertex);
            logger.fine("Processed vertex=" + vertex.getName() + " for DAG " + dag.getName());
            vertex2RunnerMap.put(vertex, next);
            for (Edge edge : edges) {
                join(vertex2RunnerMap.get(edge.getInputVertex()), edge, next);
            }
        }
        logger.fine("Processed vertices for DAG " + dag.getName());
        JobConfig jobConfig = jobContext.getJobConfig();
        long secondsToAwait = jobConfig.getSecondsToAwait();
        jobManager.deployNetworkEngine();
        logger.fine("Deployed network engine for DAG " + dag.getName());
        for (VertexRunner runner : jobManager.runners()) {
            runner.handleRequest(new VertexRunnerStartRequest()).get(secondsToAwait, TimeUnit.SECONDS);
        }
    }

    private VertexRunner createRunner(Vertex vertex) {
        VertexRunner vertexRunner = new VertexRunner(idGenerator.incrementAndGet(), vertex, jobContext);
        jobManager.registerRunner(vertex, vertexRunner);
        return vertexRunner;
    }

    private VertexRunner join(VertexRunner runner, Edge edge, VertexRunner nextRunner) {
        linkWithChannel(runner, nextRunner, edge);
        return nextRunner;
    }

    private void linkWithChannel(VertexRunner runner, VertexRunner nextRunner, Edge edge) {
        if (runner != null && nextRunner != null) {
            DataChannel channel = new DataChannel(runner, nextRunner, edge);
            runner.addOutputChannel(channel);
            nextRunner.addInputChannel(channel);
        }
    }
}
