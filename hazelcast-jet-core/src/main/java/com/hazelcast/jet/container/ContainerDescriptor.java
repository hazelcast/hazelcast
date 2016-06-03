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

import com.hazelcast.jet.application.ApplicationListener;
import com.hazelcast.jet.config.JetApplicationConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.io.ObjectReaderFactory;
import com.hazelcast.jet.io.ObjectWriterFactory;
import com.hazelcast.jet.data.tuple.JetTupleFactory;
import com.hazelcast.spi.NodeEngine;

import java.io.Serializable;

/**
 * Interface which describe container's internal entries and let's manage container-level structures;
 */
public interface ContainerDescriptor {
    /**
     * @return - Hazelcast nodeEngine;
     */
    NodeEngine getNodeEngine();

    /**
     * @return - name of the application;
     */
    String getApplicationName();

    /**
     * @return - id of the container;
     */
    int getID();

    /**
     * @return - vertex in DAG which represents corresponding container;
     */
    Vertex getVertex();

    /**
     * @return - DAG of JET-application;
     */
    DAG getDAG();

    /**
     * @return - factory to construct tuples;
     */
    JetTupleFactory getTupleFactory();

    /**
     * @return - application of JET-config;
     */
    JetApplicationConfig getConfig();

    /**
     * Performs registration of container listener;
     *
     * @param vertexName        - name of the DAG-vertex;
     * @param containerListener - list of the container;
     */
    void registerContainerListener(String vertexName,
                                   ContainerListener containerListener);

    /**
     * Performs registration of application listener;
     *
     * @param applicationListener - corresponding application listener;
     */
    void registerApplicationListener(ApplicationListener applicationListener);

    /**
     * Set-up application variable;
     *
     * @param variableName - name of the variable;
     * @param variable     - corresponding variable;
     * @param <T>          - type of the variable;
     */
    <T> void putApplicationVariable(String variableName, T variable);

    /**
     * Return variable registered on application-level;
     *
     * @param variableName - name of the variable;
     * @param <T>          - type of the variable;
     * @return - variable;
     */
    <T> T getApplicationVariable(String variableName);

    /**
     * @param variableName - name of the application;
     */
    void cleanApplicationVariable(String variableName);

    /**
     * @return - factory to construct serialization-reader factories;
     */
    ObjectReaderFactory getObjectReaderFactory();

    /**
     * @return - factory to construct serialization-writer factories;
     */
    ObjectWriterFactory getObjectWriterFactory();

    /**
     * Return accumulator to collect statistics;
     *
     * @param counterKey - key which represents counter;
     * @param <V>        - input accumulator type;
     * @param <R>        - output accumulator type;
     * @return - accumulator;
     */
    <V, R extends Serializable> Accumulator<V, R> getAccumulator(CounterKey counterKey);

    /**
     * @param counterKey  - key which represents corresponding accumulator;
     * @param accumulator - corresponding accumulator object;
     * @param <V>         - input accumulator type;
     * @param <R>         - output accumulator type;
     */
    <V, R extends Serializable> void setAccumulator(CounterKey counterKey, Accumulator<V, R> accumulator);
}
