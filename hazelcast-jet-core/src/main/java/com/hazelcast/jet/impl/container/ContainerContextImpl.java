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
import com.hazelcast.jet.container.ContainerContext;
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

public class ContainerContextImpl implements ContainerContext {
    private final int id;
    private final Vertex vertex;
    private final NodeEngine nodeEngine;
    private final JobContext jobContext;
    private final ConcurrentMap<String, Accumulator> accumulatorMap;

    public ContainerContextImpl(
            NodeEngine nodeEngine, JobContext jobContext, int id, Vertex vertex
    ) {
        this.id = id;
        this.vertex = vertex;
        this.nodeEngine = nodeEngine;
        this.jobContext = jobContext;
        this.accumulatorMap = new ConcurrentHashMap<>();
        jobContext.registerAccumulators(this.accumulatorMap);
    }

    /**
         * @return Hazelcast nodeEngine
         */
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    /**
     * @return - JET-job context;
     */
    public JobContext getJobContext() {
        return jobContext;
    }

    /**
         * @return name of the job
         */
    public String getJobName() {
        return jobContext.getName();
    }

    /**
         * @return id of the container
         */
    public int getID() {
        return id;
    }

    /**
         * @return vertex in DAG which represents corresponding container
         */
    public Vertex getVertex() {
        return vertex;
    }

    /**
         * @return DAG of JET-job
         */
    public DAG getDAG() {
        return jobContext.getDAG();
    }

    /**
         * @return job of JET-config
         */
    public JobConfig getConfig() {
        return jobContext.getJobConfig();
    }

    /**
         * Performs registration of container listener
         *
         * @param vertexName        name of the DAG-vertex
         * @param containerListener list of the container
         */
    public void registerContainerListener(String vertexName, ContainerListener containerListener) {
        jobContext.registerContainerListener(vertexName, containerListener);
    }

    /**
         * Performs registration of job listener
         *
         * @param jobListener corresponding job listener
         */
    public void registerJobListener(JobListener jobListener) {
        jobContext.registerJobListener(jobListener);
    }

    /**
         * Set-up job variable
         *
         * @param variableName name of the variable
         * @param variable     corresponding variable
         * @param <T>          type of the variable
         */
    public <T> void putJobVariable(String variableName, T variable) {
        jobContext.putJobVariable(variableName, variable);
    }

    /**
         * Return variable registered on job-level
         *
         * @param variableName name of the variable
         * @param <T>          type of the variable
         * @return variable
         */
    public <T> T getJobVariable(String variableName) {
        return jobContext.getJobVariable(variableName);
    }

    /**
         * @param variableName name of the job
         */
    public void cleanJobVariable(String variableName) {
        jobContext.cleanJobVariable(variableName);
    }

    /**
         * Return accumulator to collect statistics
         *
         * @param key key which represents counter
         * @param <V>        input accumulator type
         * @param <R>        output accumulator type
         * @return accumulator
         */
    @SuppressWarnings("unchecked")
    public <V, R extends Serializable> Accumulator<V, R> getAccumulator(String key) {
        return accumulatorMap.get(key);
    }

    /**
         * @param key  key which represents corresponding accumulator
         * @param accumulator corresponding accumulator object
         * @param <V>         input accumulator type
         * @param <R>         output accumulator type
         */
    public <V, R extends Serializable> void setAccumulator(String key, Accumulator<V, R> accumulator) {
        accumulatorMap.put(key, accumulator);
    }
}
