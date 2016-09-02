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

import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.io.SerializationOptimizer;

import java.io.Serializable;

/**
 * The context for the currently executing task
 */
public interface TaskContext {

    /**
     * Returns the task number for the current task. The returned value will be a number from 0 to the
     * <i>parallelism</i> value of the vertex.
     */
    int getTaskNumber();

    /**
     * Returns the Vertex for this task
     */
    Vertex getVertex();

    /**
     * Returns the context for the Job
     */
    JobContext getJobContext();

    /**
     * Accumulator for the statistics gathering
     *
     * @param key key of the accumulator
     * @param <V> type of the accumulator's input value
     * @param <R> type of the accumulator's output value
     * @return corresponding accumulator
     */
    <V, R extends Serializable> Accumulator<V, R> getAccumulator(String key);

    /**
     * Set new accumulator assigned to corresponding key
     *
     * @param key         key to be assigned
     * @param accumulator corresponding accumulator
     * @param <V>         type of the accumulator's input value
     * @param <R>         type of the accumulator's output value
     */
    <V, R extends Serializable> void setAccumulator(String key, Accumulator<V, R> accumulator);

    /**
     * Return the serialization optimizer
     */
    SerializationOptimizer getSerializationOptimizer();

    /**
     * Return the processor associated with this task
     */
    Processor getProcessor();
}
