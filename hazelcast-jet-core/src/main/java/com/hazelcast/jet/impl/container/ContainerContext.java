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

package com.hazelcast.jet.impl.container;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.container.ContainerListener;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.job.JobListener;
import com.hazelcast.spi.NodeEngine;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ContainerContext implements ContainerDescriptor {
    private final int id;
    private final Vertex vertex;
    private final NodeEngine nodeEngine;
    private final JobContext jobContext;
    private final ConcurrentMap<String, Accumulator> accumulatorMap;

    public ContainerContext(
            NodeEngine nodeEngine, JobContext jobContext, int id, Vertex vertex
    ) {
        this.id = id;
        this.vertex = vertex;
        this.nodeEngine = nodeEngine;
        this.jobContext = jobContext;
        this.accumulatorMap = new ConcurrentHashMap<>();
        jobContext.registerAccumulators(this.accumulatorMap);
    }

    @Override
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    /**
     * @return - JET-job context;
     */
    public JobContext getJobContext() {
        return jobContext;
    }

    @Override
    public String getJobName() {
        return jobContext.getName();
    }

    @Override
    public int getID() {
        return id;
    }

    @Override
    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public DAG getDAG() {
        return jobContext.getDAG();
    }

    @Override
    public JobConfig getConfig() {
        return jobContext.getJobConfig();
    }

    @Override
    public void registerContainerListener(String vertexName,
                                          ContainerListener containerListener) {
        jobContext.registerContainerListener(vertexName, containerListener);
    }

    @Override
    public void registerJobListener(JobListener jobListener) {
        jobContext.registerJobListener(jobListener);
    }

    @Override
    public <T> void putJobVariable(String variableName, T variable) {
        jobContext.putJobVariable(variableName, variable);
    }

    @Override
    public <T> T getJobVariable(String variableName) {
        return jobContext.getJobVariable(variableName);
    }

    public void cleanJobVariable(String variableName) {
        jobContext.cleanJobVariable(variableName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V, R extends Serializable> Accumulator<V, R> getAccumulator(String key) {
        return accumulatorMap.get(key);
    }

    @Override
    public <V, R extends Serializable> void setAccumulator(String key,
                                                           Accumulator<V, R> accumulator) {
        accumulatorMap.put(key, accumulator);
    }
}
