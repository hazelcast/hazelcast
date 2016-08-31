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

package com.hazelcast.jet.processor;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.executor.TaskContext;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.spi.NodeEngine;
import java.io.Serializable;

/**
 * Contains accessors to the resources
 */
public class ProcessorContext implements TaskContext {
    private Vertex vertex;
    private final JobContext jobContext;
    private TaskContext taskContext;

    /**
     * @param vertex      vertex
     * @param jobContext  job context
     * @param taskContext task context
     */
    public ProcessorContext(Vertex vertex, JobContext jobContext, TaskContext taskContext) {
        this.vertex = vertex;
        this.jobContext = jobContext;
        this.taskContext = taskContext;
    }

    /**
     * @return node engine
     */
    public NodeEngine getNodeEngine() {
        return jobContext.getNodeEngine();
    }

    /**
     * @return name of the job
     */
    public String getJobName() {
        return jobContext.getName();
    }

    /**
     * @return vertex in DAG
     */
    public Vertex getVertex() {
        return vertex;
    }

    /**
     * @return DAG of the job
     */
    public DAG getDAG() {
        return jobContext.getDAG();
    }

    /**
     * @return job config
     */
    public JobConfig getConfig() {
        return jobContext.getJobConfig();
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

    @Override
    public SerializationOptimizer getSerializationOptimizer() {
        return taskContext.getSerializationOptimizer();
    }
}
