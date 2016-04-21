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

package com.hazelcast.jet.impl.processor.context;

import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.spi.application.ApplicationListener;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.spi.container.ContainerListener;
import com.hazelcast.jet.spi.container.CounterKey;
import com.hazelcast.jet.spi.counters.Accumulator;
import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.data.io.ObjectReaderFactory;
import com.hazelcast.jet.spi.data.io.ObjectWriterFactory;
import com.hazelcast.jet.spi.data.tuple.JetTupleFactory;
import com.hazelcast.spi.NodeEngine;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultContainerContext implements ContainerContext {
    private final int id;
    private final Vertex vertex;
    private final NodeEngine nodeEngine;
    private final JetTupleFactory tupleFactory;
    private final ApplicationContext applicationContext;
    private final ConcurrentMap<CounterKey, Accumulator> accumulatorMap;

    public DefaultContainerContext(NodeEngine nodeEngine,
                                   ApplicationContext applicationContext,
                                   int id,
                                   Vertex vertex,
                                   JetTupleFactory tupleFactory) {
        this.id = id;
        this.vertex = vertex;
        this.nodeEngine = nodeEngine;
        this.tupleFactory = tupleFactory;
        this.applicationContext = applicationContext;
        this.accumulatorMap = new ConcurrentHashMap<CounterKey, Accumulator>();
        applicationContext.registerAccumulators(this.accumulatorMap);
    }

    @Override
    public NodeEngine getNodeEngine() {
        return this.nodeEngine;
    }

    @Override
    public ApplicationContext getApplicationContext() {
        return this.applicationContext;
    }

    @Override
    public String getApplicationName() {
        return this.applicationContext.getName();
    }

    @Override
    public int getID() {
        return this.id;
    }

    @Override
    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public DAG getDAG() {
        return this.applicationContext.getDAG();
    }

    @Override
    public JetTupleFactory getTupleFactory() {
        return this.tupleFactory;
    }

    @Override
    public JetApplicationConfig getConfig() {
        return this.applicationContext.getJetApplicationConfig();
    }

    @Override
    public void registerContainerListener(String vertexName,
                                          ContainerListener containerListener) {
        this.applicationContext.registerContainerListener(vertexName, containerListener);
    }

    @Override
    public void registerApplicationListener(ApplicationListener applicationListener) {
        this.applicationContext.registerApplicationListener(applicationListener);
    }

    @Override
    public <T> void putApplicationVariable(String variableName, T variable) {
        this.applicationContext.putApplicationVariable(variableName, variable);
    }

    @Override
    public <T> T getApplicationVariable(String variableName) {
        return this.applicationContext.getApplicationVariable(variableName);
    }

    public void cleanApplicationVariable(String variableName) {
        this.applicationContext.cleanApplicationVariable(variableName);
    }

    @Override
    public ObjectReaderFactory getObjectReaderFactory() {
        return this.applicationContext.getIOContext().getObjectReaderFactory();
    }

    @Override
    public ObjectWriterFactory getObjectWriterFactory() {
        return this.applicationContext.getIOContext().getObjectWriterFactory();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V, R extends Serializable> Accumulator<V, R> getAccumulator(CounterKey counterKey) {
        return this.accumulatorMap.get(counterKey);
    }

    @Override
    public <V, R extends Serializable> void setAccumulator(CounterKey counterKey,
                                                           Accumulator<V, R> accumulator) {
        this.accumulatorMap.put(counterKey, accumulator);
    }
}
