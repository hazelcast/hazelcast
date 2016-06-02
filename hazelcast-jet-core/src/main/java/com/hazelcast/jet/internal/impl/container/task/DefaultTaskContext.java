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

package com.hazelcast.jet.internal.impl.container.task;

import com.hazelcast.jet.internal.api.application.ApplicationContext;
import com.hazelcast.jet.internal.impl.data.io.JetTupleDataType;
import com.hazelcast.jet.io.api.IOContext;
import com.hazelcast.jet.io.internal.impl.IOContextImpl;
import com.hazelcast.jet.api.container.CounterKey;
import com.hazelcast.jet.api.counters.Accumulator;
import com.hazelcast.jet.io.api.DataType;
import com.hazelcast.jet.io.api.ObjectReaderFactory;
import com.hazelcast.jet.io.api.ObjectWriterFactory;
import com.hazelcast.jet.api.executor.TaskContext;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultTaskContext implements TaskContext {
    private final int taskCount;
    private final int taskNumber;
    private final IOContext ioContext;
    private final ConcurrentMap<CounterKey, Accumulator> accumulatorMap;

    public DefaultTaskContext(int taskCount,
                              int taskNumber,
                              ApplicationContext applicationContext) {
        this.taskCount = taskCount;
        this.taskNumber = taskNumber;
        this.ioContext = new IOContextImpl(JetTupleDataType.INSTANCE);
        this.accumulatorMap = new ConcurrentHashMap<CounterKey, Accumulator>();
        applicationContext.registerAccumulators(this.accumulatorMap);
    }

    @Override
    public int getTaskCount() {
        return this.taskCount;
    }

    @Override
    public int getTaskNumber() {
        return this.taskNumber;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V, R extends Serializable> Accumulator<V, R> getAccumulator(CounterKey counterKey) {
        return this.accumulatorMap.get(counterKey);
    }

    @Override
    public <V, R extends Serializable> void setAccumulator(CounterKey counterKey, Accumulator<V, R> accumulator) {
        this.accumulatorMap.put(counterKey, accumulator);
    }

    @Override
    public void registerDataType(DataType dataType) {
        this.ioContext.registerDataType(dataType);
    }

    @Override
    public ObjectReaderFactory getObjectReaderFactory() {
        return this.ioContext.getObjectReaderFactory();
    }

    @Override
    public ObjectWriterFactory getObjectWriterFactory() {
        return this.ioContext.getObjectWriterFactory();
    }
}
