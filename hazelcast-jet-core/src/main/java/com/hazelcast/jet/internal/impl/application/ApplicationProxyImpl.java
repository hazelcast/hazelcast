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

package com.hazelcast.jet.internal.impl.application;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.internal.api.application.ApplicationClusterService;
import com.hazelcast.jet.internal.api.application.ApplicationProxy;
import com.hazelcast.jet.internal.api.application.ApplicationStateManager;
import com.hazelcast.jet.internal.api.application.Initable;
import com.hazelcast.jet.internal.api.hazelcast.JetService;
import com.hazelcast.jet.internal.api.statemachine.application.ApplicationState;
import com.hazelcast.jet.internal.impl.util.JetThreadFactory;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.container.CounterKey;
import com.hazelcast.jet.api.counters.Accumulator;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class ApplicationProxyImpl extends AbstractDistributedObject<JetService> implements ApplicationProxy, Initable {
    private final String name;
    private final HazelcastInstance hazelcastInstance;
    private final Set<LocalizationResource> localizedResources;
    private final ApplicationStateManager applicationStateManager;
    private final ApplicationClusterService applicationClusterService;

    public ApplicationProxyImpl(String name, JetService jetService, NodeEngine nodeEngine) {
        super(nodeEngine, jetService);

        this.name = name;
        this.localizedResources = new HashSet<LocalizationResource>();
        String hzName = nodeEngine.getHazelcastInstance().getName();

        ExecutorService executorService = Executors.newCachedThreadPool(
                new JetThreadFactory("invoker-application-thread-" + this.name, hzName)
        );

        this.hazelcastInstance = nodeEngine.getHazelcastInstance();
        this.applicationStateManager = new DefaultApplicationStateManager(name);

        this.applicationClusterService = new ServerApplicationClusterService(
                name, executorService,
                nodeEngine, jetService
        );
    }

    @Override
    public void init(JetApplicationConfig config) {
        this.applicationClusterService.initApplication(config, this.applicationStateManager);
    }

    @Override
    public void submit(DAG dag, Class... classes) throws IOException {
        if (classes != null) {
            addResource(classes);
        }

        localizeApplication();
        submit0(dag);
    }

    @Override
    public Future execute() {
        return this.applicationClusterService.executeApplication(this.applicationStateManager);
    }

    @Override
    public Future interrupt() {
        return this.applicationClusterService.interruptApplication(this.applicationStateManager);
    }

    @Override
    public Future finalizeApplication() {
        return this.applicationClusterService.finalizeApplication(this.applicationStateManager);
    }

    @Override
    public void addResource(Class... classes) throws IOException {
        checkNotNull(classes, "Classes can not be null");

        for (Class clazz : classes) {
            this.localizedResources.add(new LocalizationResource(clazz));
        }
    }

    @Override
    public void addResource(URL url) throws IOException {
        this.localizedResources.add(new LocalizationResource(url));
    }

    @Override
    public void addResource(InputStream inputStream, String name, LocalizationResourceType resourceType) throws IOException {
        this.localizedResources.add(new LocalizationResource(inputStream, name, resourceType));
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void clearResources() {
        this.localizedResources.clear();
    }

    @Override
    public ApplicationState getApplicationState() {
        return this.applicationStateManager.getApplicationState();
    }

    @Override
    public String getServiceName() {
        return JetService.SERVICE_NAME;
    }

    @Override
    public Map<CounterKey, Accumulator> getAccumulators() {
        return this.applicationClusterService.getAccumulators();
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        return this.hazelcastInstance;
    }

    private void localizeApplication() {
        this.applicationClusterService.localizeApplication(this.localizedResources, this.applicationStateManager);
    }

    private void submit0(final DAG dag) {
        this.applicationClusterService.submitDag(dag, this.applicationStateManager);
    }
}
