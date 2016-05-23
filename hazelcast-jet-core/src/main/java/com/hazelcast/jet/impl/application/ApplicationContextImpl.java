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
import com.hazelcast.jet.api.JetApplicationManager;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.application.ExecutorContext;
import com.hazelcast.jet.impl.data.io.JetTupleDataType;
import com.hazelcast.jet.io.impl.IOContextImpl;
import com.hazelcast.jet.io.spi.IOContext;
import com.hazelcast.jet.api.application.localization.LocalizationStorage;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.api.data.io.SocketReader;
import com.hazelcast.jet.api.data.io.SocketWriter;
import com.hazelcast.jet.api.statemachine.ApplicationStateMachine;
import com.hazelcast.jet.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.api.statemachine.application.ApplicationEvent;
import com.hazelcast.jet.api.statemachine.application.ApplicationStateMachineFactory;
import com.hazelcast.jet.impl.application.localization.LocalizationStorageFactory;
import com.hazelcast.jet.impl.container.ApplicationMasterImpl;
import com.hazelcast.jet.impl.container.DefaultDiscoveryService;
import com.hazelcast.jet.impl.statemachine.application.ApplicationStateMachineImpl;
import com.hazelcast.jet.impl.statemachine.application.DefaultApplicationStateMachineRequestProcessor;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.application.ApplicationListener;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.spi.container.ContainerListener;
import com.hazelcast.jet.spi.container.CounterKey;
import com.hazelcast.jet.spi.counters.Accumulator;
import com.hazelcast.jet.spi.dag.DAG;
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

public class ApplicationContextImpl implements ApplicationContext {
    private static final ApplicationStateMachineFactory STATE_MACHINE_FACTORY =
            new ApplicationStateMachineFactory() {
                @Override
                public ApplicationStateMachine newStateMachine(String name,
                                                               StateMachineRequestProcessor<ApplicationEvent> processor,
                                                               NodeEngine nodeEngine,
                                                               ApplicationContext applicationContext) {
                    return new ApplicationStateMachineImpl(
                            name,
                            processor,
                            nodeEngine,
                            applicationContext
                    );
                }
            };

    private static final IFunction<String, List<ContainerListener>> FUNCTION_FACTORY =
            new IFunction<String, List<ContainerListener>>() {
                @Override
                public List<ContainerListener> apply(String input) {
                    return new CopyOnWriteArrayList<ContainerListener>();
                }
            };

    private final String name;

    private final NodeEngine nodeEngine;
    private final AtomicReference<Address> owner;
    private final AtomicInteger containerIdGenerator;
    private final ApplicationMaster applicationMaster;
    private final LocalizationStorage localizationStorage;
    private final Map<Address, Address> hzToAddressMapping;
    private final JetApplicationConfig jetApplicationConfig;
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

    private final List<ConcurrentMap<CounterKey, Accumulator>> accumulators;

    public ApplicationContextImpl(String name,
                                  NodeEngine nodeEngine,
                                  Address localJetAddress,
                                  JetApplicationConfig jetApplicationConfig,
                                  JetApplicationManager jetApplicationManager) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.localJetAddress = localJetAddress;
        this.owner = new AtomicReference<Address>();
        this.containerIdGenerator = new AtomicInteger(0);

        this.jetApplicationConfig = JetUtil.resolveJetServerApplicationConfig(
                nodeEngine,
                jetApplicationConfig,
                name
        );

        this.executorContext = new DefaultExecutorContext(
                this.name,
                this.jetApplicationConfig,
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

        this.hzToAddressMapping = new HashMap<Address, Address>();
        this.accumulators = new CopyOnWriteArrayList<ConcurrentMap<CounterKey, Accumulator>>();
        this.applicationMaster = createApplicationMaster(nodeEngine);
        this.ioContext = new IOContextImpl(JetTupleDataType.INSTANCE);
    }

    private ApplicationMasterImpl createApplicationMaster(NodeEngine nodeEngine) {
        return new ApplicationMasterImpl(
                this,
                new DefaultDiscoveryService(
                        this,
                        nodeEngine,
                        this.socketWriters,
                        this.socketReaders,
                        this.hzToAddressMapping
                )
        );
    }

    @Override
    public boolean validateOwner(Address applicationOwner) {
        return (this.owner.compareAndSet(null, applicationOwner))
                ||
                (this.owner.compareAndSet(applicationOwner, applicationOwner));
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Address getOwner() {
        return this.owner.get();
    }

    @Override
    public LocalizationStorage getLocalizationStorage() {
        return this.localizationStorage;
    }

    @Override
    public ApplicationStateMachine getApplicationStateMachine() {
        return this.applicationStateMachine;
    }

    @Override
    public ApplicationMaster getApplicationMaster() {
        return applicationMaster;
    }

    @Override
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public JetApplicationConfig getJetApplicationConfig() {
        return this.jetApplicationConfig;
    }

    @Override
    public void registerApplicationListener(ApplicationListener applicationListener) {
        this.applicationListeners.add(applicationListener);
    }

    @Override
    public ConcurrentMap<String, List<ContainerListener>> getContainerListeners() {
        return this.containerListeners;
    }

    @Override
    public List<ApplicationListener> getApplicationListeners() {
        return this.applicationListeners;
    }

    @Override
    public AtomicInteger getContainerIDGenerator() {
        return this.containerIdGenerator;
    }

    @Override
    public DAG getDAG() {
        return this.applicationMaster.getDag();
    }

    @Override
    public Map<Address, Address> getHzToJetAddressMapping() {
        return this.hzToAddressMapping;
    }

    @Override
    public Map<Address, SocketWriter> getSocketWriters() {
        return this.socketWriters;
    }

    @Override
    public Map<Address, SocketReader> getSocketReaders() {
        return this.socketReaders;
    }

    @Override
    public Address getLocalJetAddress() {
        return this.localJetAddress;
    }

    @Override
    public void registerContainerListener(String vertexName,
                                          ContainerListener containerListener) {
        List<ContainerListener> listeners =
                this.containerListeners.applyIfAbsent(vertexName, FUNCTION_FACTORY);
        listeners.add(containerListener);
    }

    @Override
    public <T> void putApplicationVariable(String variableName,
                                           T variable) {
        this.applicationVariables.put(variableName, variable);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getApplicationVariable(String variableName) {
        return (T) this.applicationVariables.get(variableName);
    }

    @Override
    public void cleanApplicationVariable(String variableName) {
        this.applicationVariables.remove(variableName);
    }

    @Override
    public ExecutorContext getExecutorContext() {
        return this.executorContext;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<CounterKey, Accumulator> getAccumulators() {
        Map<CounterKey, Accumulator> map = new HashMap<CounterKey, Accumulator>();

        for (ConcurrentMap<CounterKey, Accumulator> concurrentMap : this.accumulators) {
            for (Map.Entry<CounterKey, Accumulator> entry : concurrentMap.entrySet()) {
                CounterKey counterKey = entry.getKey();
                Accumulator accumulator = entry.getValue();

                Accumulator collector = map.get(counterKey);

                if (collector == null) {
                    map.put(counterKey, accumulator);
                } else {
                    collector.merge(accumulator);
                }
            }
        }

        return map;
    }

    @Override
    public void registerAccumulators(ConcurrentMap<CounterKey, Accumulator> accumulatorMap) {
        this.accumulators.add(accumulatorMap);
    }

    @Override
    public IOContext getIOContext() {
        return this.ioContext;
    }
}
