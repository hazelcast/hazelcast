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

package com.hazelcast.jet.api.executor;

import com.hazelcast.jet.api.container.CounterKey;
import com.hazelcast.jet.api.counters.Accumulator;
import com.hazelcast.jet.io.api.DataType;
import com.hazelcast.jet.io.api.ObjectReaderFactory;
import com.hazelcast.jet.io.api.ObjectWriterFactory;

import java.io.Serializable;

/**
 * Represents task context;
 * Holds task's information;
 */
public interface TaskContext {
    /**
     * @return - general number of task in the current container;
     */
    int getTaskCount();

    /**
     * @return - number of the current task inside the container;
     */
    int getTaskNumber();

    /**
     * Accumulator for the statistics gathering;
     *
     * @param counterKey - key of the accumulator;
     * @param <V>        - type of the accumulator's input value;
     * @param <R>        - type of the accumulator's output value;
     * @return corresponding accumulator;
     */
    <V, R extends Serializable> Accumulator<V, R> getAccumulator(CounterKey counterKey);

    /**
     * Set new accumulator assigned to corresponding counterKey;
     *
     * @param counterKey  - key to be assigned;
     * @param accumulator - corresponding accumulator;
     * @param <V>         - type of the accumulator's input value;
     * @param <R>         - type of the accumulator's output value;
     */
    <V, R extends Serializable> void setAccumulator(CounterKey counterKey, Accumulator<V, R> accumulator);

    /**
     * Register dataType for serialization purposes;
     *
     * @param dataType - corresponding dataType;
     */
    void registerDataType(DataType dataType);

    /**
     * @return - factory to create objectReaders;
     */
    ObjectReaderFactory getObjectReaderFactory();

    /**
     * @return - factory to create objectWriters;
     */
    ObjectWriterFactory getObjectWriterFactory();
}
