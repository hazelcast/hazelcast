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
import com.hazelcast.jet.container.ContainerListener;
import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.executor.TaskContext;
import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.jet.job.JobListener;
import com.hazelcast.spi.NodeEngine;

import java.io.Serializable;

public class DefaultProcessorContext implements ProcessorContext {
    private final TaskContext taskContext;
    private final ContainerContextImpl containerContext;

    public DefaultProcessorContext(TaskContext taskContext,
                                   ContainerContextImpl containerContext) {
        this.taskContext = taskContext;
        this.containerContext = containerContext;
    }

    @Override
    public NodeEngine getNodeEngine() {
        return containerContext.getNodeEngine();
    }

    @Override
    public String getJobName() {
        return containerContext.getJobName();
    }

    @Override
    public int getID() {
        return containerContext.getID();
    }

    @Override
    public Vertex getVertex() {
        return containerContext.getVertex();
    }

    @Override
    public DAG getDAG() {
        return containerContext.getDAG();
    }

    @Override
    public JobConfig getConfig() {
        return containerContext.getConfig();
    }

    @Override
    public void registerContainerListener(String vertexName, ContainerListener containerListener) {
        containerContext.registerContainerListener(vertexName, containerListener);
    }

    @Override
    public void registerJobListener(JobListener jobListener) {
        containerContext.registerJobListener(jobListener);
    }

    @Override
    public <T> void putJobVariable(String variableName, T variable) {
        containerContext.putJobVariable(variableName, variable);
    }

    @Override
    public <T> T getJobVariable(String variableName) {
        return containerContext.getJobVariable(variableName);
    }

    @Override
    public void cleanJobVariable(String variableName) {
        containerContext.cleanJobVariable(variableName);
    }

    public SerializationOptimizer getSerializationOptimizer() {
        return taskContext.getSerializationOptimizer();
    }

    @Override
    public int getTaskCount() {
        return taskContext.getTaskCount();
    }

    @Override
    public int getTaskNumber() {
        return taskContext.getTaskNumber();
    }

    @Override
    public <V, R extends Serializable> Accumulator<V, R> getAccumulator(String key) {
        return taskContext.getAccumulator(key);
    }

    @Override
    public <V, R extends Serializable> void setAccumulator(String key, Accumulator<V, R> accumulator) {
        taskContext.setAccumulator(key, accumulator);
    }
}
