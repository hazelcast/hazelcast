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

package com.hazelcast.jet.impl.runtime.task;

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.impl.data.io.JetPairDataType;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.jet.runtime.TaskContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TaskContextImpl implements TaskContext {
    private final SerializationOptimizer optimizer;
    private final Map<String, Accumulator> accumulatorMap;
    private Vertex vertex;
    private final JobContext jobContext;
    private final Processor processor;
    private final int taskNumber;

    public TaskContextImpl(Vertex vertex, JobContext jobContext, Processor processor, int taskNumber) {
        this.vertex = vertex;
        this.jobContext = jobContext;
        this.processor = processor;
        this.taskNumber = taskNumber;
        this.optimizer = new SerializationOptimizer(JetPairDataType.INSTANCE);
        this.accumulatorMap = new HashMap<>();
        jobContext.registerAccumulators(this.accumulatorMap);
    }

    @Override
    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public JobContext getJobContext() {
        return jobContext;
    }

    @Override
    public int getTaskNumber() {
        return taskNumber;
    }

    @Override
    public <V, R extends Serializable> Accumulator<V, R> getAccumulator(String key) {
        return accumulatorMap.get(key);
    }

    @Override
    public <V, R extends Serializable> void setAccumulator(String key, Accumulator<V, R> accumulator) {
        accumulatorMap.put(key, accumulator);
    }

    @Override
    public SerializationOptimizer getSerializationOptimizer() {
        return optimizer;
    }

    @Override
    public Processor getProcessor() {
        return processor;
    }
}
