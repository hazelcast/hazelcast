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

package com.hazelcast.jet.impl.application;


import com.hazelcast.core.IFunction;
import com.hazelcast.jet.application.ApplicationListener;
import com.hazelcast.jet.config.ApplicationConfig;
import com.hazelcast.jet.container.ContainerListener;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.impl.application.localization.LocalizationStorage;
import com.hazelcast.jet.impl.application.localization.LocalizationStorageFactory;
import com.hazelcast.jet.impl.container.ApplicationMaster;
import com.hazelcast.jet.impl.container.DiscoveryService;
import com.hazelcast.jet.impl.data.io.JetTupleDataType;
import com.hazelcast.jet.impl.data.io.SocketReader;
import com.hazelcast.jet.impl.data.io.SocketWriter;
import com.hazelcast.jet.impl.statemachine.StateMachineFactory;
import com.hazelcast.jet.impl.statemachine.application.ApplicationEvent;
import com.hazelcast.jet.impl.statemachine.application.ApplicationStateMachine;
import com.hazelcast.jet.impl.statemachine.application.DefaultApplicationStateMachineRequestProcessor;
import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.impl.IOContextImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrentReferenceHashMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ApplicationContext {
    private static final StateMachineFactory<ApplicationEvent, ApplicationStateMachine> STATE_MACHINE_FACTORY =
            ApplicationStateMachine::new;

    private static final IFunction<String, List<ContainerListener>> FUNCTION_FACTORY =
            (IFunction<String, List<ContainerListener>>) input -> new CopyOnWriteArrayList<>();

    private final String name;

    private final NodeEngine nodeEngine;
    private final AtomicReference<Address> owner;
    private final AtomicInteger containerIdGenerator;
    private final ApplicationMaster applicationMaster;
    private final LocalizationStorage localizationStorage;
    private final Map<Address, Address> hzToAddressMapping;
    private final ApplicationConfig applicationConfig;
    private final ApplicationStateMachine applicationStateMachine;
    private final Map<String, Object> applicationVariables = new ConcurrentHashMap<String, Object>();
    private final List<ApplicationListener> applicationListeners = new CopyOnWriteArrayList<ApplicationListener>();
    private final com.hazelcast.util.IConcurrentMap<String, List<ContainerListener>> containerListeners =
            new ConcurrentReferenceHashMap<String, List<ContainerListener>>();

    private final IOContext ioContext;

    private final Address localJetAddress;

    private final ExecutorContext executorContext;

    private final Map<Address, SocketWriter> socketWriters = new HashMap<Address, SocketWriter>();

    private final Map<Address, SocketReader> socketReaders = new HashMap<Address, SocketReader>();

    private final List<ConcurrentMap<String, Accumulator>> accumulators;

    public ApplicationContext(String name,
                              NodeEngine nodeEngine,
                              Address localJetAddress,
                              ApplicationConfig applicationConfig,
                              JetApplicationManager jetApplicationManager) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.localJetAddress = localJetAddress;
        this.owner = new AtomicReference<Address>();
        this.containerIdGenerator = new AtomicInteger(0);

        this.applicationConfig = applicationConfig;

        this.executorContext = new ExecutorContext(
                this.name,
                this.applicationConfig,
                nodeEngine,
                jetApplicationManager.getNetworkExecutor(),
                jetApplicationManager.getProcessingExecutor()

        );

        this.localizationStorage = LocalizationStorageFactory.getLocalizationStorage(
                this,
                name
        );

        this.applicationStateMachine = STATE_MACHINE_FACTORY.newStateMachine(
                name,
                new DefaultApplicationStateMachineRequestProcessor(this),
                nodeEngine,
                this
        );

        this.hzToAddressMapping = new HashMap<>();
        this.accumulators = new CopyOnWriteArrayList<>();
        this.applicationMaster = createApplicationMaster(nodeEngine);
        this.ioContext = new IOContextImpl(JetTupleDataType.INSTANCE);
    }

    private ApplicationMaster createApplicationMaster(NodeEngine nodeEngine) {
        return new ApplicationMaster(
                this,
                new DiscoveryService(
                        this,
                        nodeEngine,
                        this.socketWriters,
                        this.socketReaders,
                        this.hzToAddressMapping
                )
        );
    }

    /**
     * @param applicationOwner owner of application
     * @return true if  application was created by node with address specified in @applicationOwner
     * false otherwise
     */
    public boolean validateOwner(Address applicationOwner) {
        return (this.owner.compareAndSet(null, applicationOwner))
                ||
                (this.owner.compareAndSet(applicationOwner, applicationOwner));
    }

    /**
     * @return name of the application
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return node's address which created application
     */
    public Address getOwner() {
        return this.owner.get();
    }

    /**
     * @return Localization storage for application
     */
    public LocalizationStorage getLocalizationStorage() {
        return this.localizationStorage;
    }

    /**
     * @return application's state machine
     */
    public ApplicationStateMachine getApplicationStateMachine() {
        return this.applicationStateMachine;
    }

    /**
     * @return applicationMaster
     */
    public ApplicationMaster getApplicationMaster() {
        return applicationMaster;
    }

    /**
     * @return node engine of corresponding to the current node
     */
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    /**
     * @return application's config
     */
    public ApplicationConfig getApplicationConfig() {
        return this.applicationConfig;
    }

    /**
     * Register life-cycle listener for application
     *
     * @param applicationListener corresponding listener
     */
    public void registerApplicationListener(ApplicationListener applicationListener) {
        this.applicationListeners.add(applicationListener);
    }

    /**
     * @return all registered container's listeners
     */
    public ConcurrentMap<String, List<ContainerListener>> getContainerListeners() {
        return this.containerListeners;
    }

    /**
     * @return all registered application's listeners
     */
    public List<ApplicationListener> getApplicationListeners() {
        return this.applicationListeners;
    }

    /**
     * @return generator for the container's ids
     */
    public AtomicInteger getContainerIDGenerator() {
        return this.containerIdGenerator;
    }

    /**
     * @return direct acyclic graph corresponding to application
     */
    public DAG getDAG() {
        return this.applicationMaster.getDag();
    }

    /**
     * @return mapping between main Hazelcast and Jet addresses
     */
    public Map<Address, Address> getHzToJetAddressMapping() {
        return this.hzToAddressMapping;
    }

    /**
     * @return map of socket Writers
     */
    public Map<Address, SocketWriter> getSocketWriters() {
        return this.socketWriters;
    }

    /**
     * @return map of socket readers
     */
    public Map<Address, SocketReader> getSocketReaders() {
        return this.socketReaders;
    }

    /**
     * @return Jet's server address for the current node
     */
    public Address getLocalJetAddress() {
        return this.localJetAddress;
    }

    /**
     * Register container listener for the corresponding vertex
     *
     * @param vertexName        name of the corresponding vertex
     * @param containerListener container listener
     */
    public void registerContainerListener(String vertexName,
                                          ContainerListener containerListener) {
        List<ContainerListener> listeners =
                this.containerListeners.applyIfAbsent(vertexName, FUNCTION_FACTORY);
        listeners.add(containerListener);
    }

    /**
     * Set up application-local variable
     *
     * @param variableName name of the variable
     * @param variable     value of the variable
     * @param <T>          type of the variable
     */
    public <T> void putApplicationVariable(String variableName,
                                           T variable) {
        this.applicationVariables.put(variableName, variable);
    }

    /**
     * Return the value of the application-local variable
     *
     * @param variableName name of the variable
     * @param <T>          type of the variable
     * @return value of the variable
     */
    @SuppressWarnings("unchecked")
    public <T> T getApplicationVariable(String variableName) {
        return (T) this.applicationVariables.get(variableName);
    }

    /**
     * Clean application-local variable
     *
     * @param variableName variable name
     */
    public void cleanApplicationVariable(String variableName) {
        this.applicationVariables.remove(variableName);
    }

    /**
     * @return application Executor context which provides thread-pooling management
     */
    public ExecutorContext getExecutorContext() {
        return this.executorContext;
    }

    /**
     * @return map with accumulators
     */
    @SuppressWarnings("unchecked")
    public Map<String, Accumulator> getAccumulators() {
        Map<String, Accumulator> map = new HashMap<String, Accumulator>();

        for (ConcurrentMap<String, Accumulator> concurrentMap : this.accumulators) {
            for (Map.Entry<String, Accumulator> entry : concurrentMap.entrySet()) {
                String key = entry.getKey();
                Accumulator accumulator = entry.getValue();

                Accumulator collector = map.get(key);

                if (collector == null) {
                    map.put(key, accumulator);
                } else {
                    collector.merge(accumulator);
                }
            }
        }

        return map;
    }

    /**
     * @param accumulatorMap map with accumulators
     */
    public void registerAccumulators(ConcurrentMap<String, Accumulator> accumulatorMap) {
        this.accumulators.add(accumulatorMap);
    }

    /**
     * @return IOContext of application which provides IO-management
     */
    public IOContext getIOContext() {
        return this.ioContext;
    }
}
