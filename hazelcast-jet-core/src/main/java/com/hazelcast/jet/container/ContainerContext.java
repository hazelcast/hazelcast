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

package com.hazelcast.jet.container;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.job.JobListener;
import com.hazelcast.spi.NodeEngine;

import java.io.Serializable;

/**
 * Interface which describe container's internal entries and let's manage container-level structures
 */
public interface ContainerContext {
    /**
     * @return Hazelcast nodeEngine
     */
    NodeEngine getNodeEngine();

    /**
     * @return name of the job
     */
    String getJobName();

    /**
     * @return id of the container
     */
    int getID();

    /**
     * @return vertex in DAG which represents corresponding container
     */
    Vertex getVertex();

    /**
     * @return DAG of JET-job
     */
    DAG getDAG();

    /**
     * @return job of JET-config
     */
    JobConfig getConfig();

    /**
     * Performs registration of container listener
     *
     * @param vertexName        name of the DAG-vertex
     * @param containerListener list of the container
     */
    void registerContainerListener(String vertexName,
                                   ContainerListener containerListener);

    /**
     * Performs registration of job listener
     *
     * @param jobListener corresponding job listener
     */
    void registerJobListener(JobListener jobListener);

    /**
     * Set-up job variable
     *
     * @param variableName name of the variable
     * @param variable     corresponding variable
     * @param <T>          type of the variable
     */
    <T> void putJobVariable(String variableName, T variable);

    /**
     * Return variable registered on job-level
     *
     * @param variableName name of the variable
     * @param <T>          type of the variable
     * @return variable
     */
    <T> T getJobVariable(String variableName);

    /**
     * @param variableName name of the job
     */
    void cleanJobVariable(String variableName);

    /**
     * Return accumulator to collect statistics
     *
     * @param key key which represents counter
     * @param <V>        input accumulator type
     * @param <R>        output accumulator type
     * @return accumulator
     */
    <V, R extends Serializable> Accumulator<V, R> getAccumulator(String key);

    /**
     * @param key  key which represents corresponding accumulator
     * @param accumulator corresponding accumulator object
     * @param <V>         input accumulator type
     * @param <R>         output accumulator type
     */
    <V, R extends Serializable> void setAccumulator(String key, Accumulator<V, R> accumulator);
}
