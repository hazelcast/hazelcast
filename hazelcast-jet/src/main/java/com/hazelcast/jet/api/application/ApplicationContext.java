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

package com.hazelcast.jet.api.application;

import java.util.Map;
import java.util.List;

import com.hazelcast.jet.spi.application.ApplicationListener;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.spi.dag.DAG;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.jet.api.data.io.SocketReader;
import com.hazelcast.jet.api.data.io.SocketWriter;
import com.hazelcast.jet.spi.container.CounterKey;
import com.hazelcast.jet.spi.counters.Accumulator;
import com.hazelcast.jet.spi.container.ContainerListener;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.api.statemachine.ApplicationStateMachine;
import com.hazelcast.jet.api.application.localization.LocalizationStorage;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;

/**
 * ApplicationContext which represents context of the certain application;
 * Used during calculation process;
 */
public interface ApplicationContext {
    /**
     * @return direct acyclic graph corresponding to application;
     */
    DAG getDAG();

    /**
     * @return name of the application;
     */
    String getName();

    /**
     * @return node's address which created application;
     */
    Address getOwner();

    /**
     * @return node engine of corresponding to the current node;
     */
    NodeEngine getNodeEngine();

    /**
     * @return generator for the container's ids;
     */
    AtomicInteger getContainerIDGenerator();

    /**
     * @return applicationMaster;
     */
    ApplicationMaster getApplicationMaster();

    /**
     * @param applicationOwner - owner of application;
     * @return true -
     * if  application was created by node with address specified in @applicationOwner;
     * false  -
     * otherwise
     */
    boolean validateOwner(Address applicationOwner);

    /**
     * @return - Localization storage for application;
     */
    LocalizationStorage getLocalizationStorage();

    /**
     * @return - application's config;
     */
    JetApplicationConfig getJetApplicationConfig();

    /**
     * @return - application's state machine;
     */
    ApplicationStateMachine getApplicationStateMachine();

    /**
     * Register life-cycle listener for application
     *
     * @param applicationListener - corresponding listener
     */
    void registerApplicationListener(ApplicationListener applicationListener);

    /**
     * @return - all registered container's listeners
     */
    ConcurrentMap<String, List<ContainerListener>> getContainerListeners();

    /**
     * @return - all registered application's listeners
     */
    List<ApplicationListener> getApplicationListeners();

    /**
     * @return - mapping between main Hazelcast and Jet addresses;
     */
    Map<Address, Address> getHzToJetAddressMapping();

    /**
     * @return - map of socket Writers
     */
    Map<Address, SocketWriter> getSocketWriters();

    /**
     * @return - map of socket readers
     */
    Map<Address, SocketReader> getSocketReaders();

    /**
     * @return - Jet's server address for the current node
     */
    Address getLocalJetAddress();

    /**
     * Register container listener for the corresponding vertex
     *
     * @param vertexName        - name of the corresponding vertex
     * @param containerListener -   container listener
     */
    void registerContainerListener(String vertexName,
                                   ContainerListener containerListener);

    /**
     * Set up application-local variable;
     *
     * @param variableName -   name of the variable;
     * @param variable     -   value of the variable;
     * @param <T>          -   type of the variable;
     */
    <T> void putApplicationVariable(String variableName, T variable);

    /**
     * Return the value of the application-local variable;
     *
     * @param variableName - name of the variable;
     * @param <T>          - type of the variable;
     * @return value of the variable
     */
    <T> T getApplicationVariable(String variableName);

    /**
     * Clean application-local variable;
     *
     * @param variableName - variable name;
     */
    void cleanApplicationVariable(String variableName);

    /**
     * @return - application Executor context which provides thread-pooling management;
     */
    ExecutorContext getExecutorContext();

    /**
     * @return - map with accumulators;
     */
    Map<CounterKey, Accumulator> getAccumulators();

    /**
     * @param accumulatorMap - map with accumulators
     */
    void registerAccumulators(ConcurrentMap<CounterKey, Accumulator> accumulatorMap);

    /**
     * @return - IOContext of application which provides IO-management;
     */
    IOContext getIOContext();
}
