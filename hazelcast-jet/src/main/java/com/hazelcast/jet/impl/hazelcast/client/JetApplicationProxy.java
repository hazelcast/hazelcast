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

package com.hazelcast.jet.impl.hazelcast.client;

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.api.application.ApplicationClusterService;
import com.hazelcast.jet.api.application.ApplicationProxy;
import com.hazelcast.jet.api.application.ApplicationStateManager;
import com.hazelcast.jet.api.application.Initable;
import com.hazelcast.jet.api.statemachine.application.ApplicationState;
import com.hazelcast.jet.impl.application.DefaultApplicationStateManager;
import com.hazelcast.jet.impl.application.LocalizationResource;
import com.hazelcast.jet.impl.application.LocalizationResourceType;
import com.hazelcast.jet.impl.util.JetThreadFactory;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.spi.container.CounterKey;
import com.hazelcast.jet.spi.counters.Accumulator;
import com.hazelcast.jet.spi.dag.DAG;

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

public class JetApplicationProxy extends ClientProxy implements ApplicationProxy, Initable {
    private final Set<LocalizationResource> localizedResources;
    private final ApplicationStateManager applicationStateManager;
    private ApplicationClusterService applicationClusterService;

    public JetApplicationProxy(String serviceName,
                               String name) {
        super(serviceName, name);
        this.localizedResources = new HashSet<LocalizationResource>();
        this.applicationStateManager = new DefaultApplicationStateManager(name);
    }

    protected void onInitialize() {
        String hzName = getClient().getName();

        ExecutorService executorService = Executors.newCachedThreadPool(
                new JetThreadFactory("client-invoker-application-thread-" + this.name, hzName)
        );

        this.applicationClusterService = new ClientApplicationClusterService(
                getClient(),
                this.name,
                executorService
        );
    }

    @Override
    public void init(JetApplicationConfig config) {
        this.applicationClusterService.initApplication(config, this.applicationStateManager);
    }

    private void localizeApplication() {
        this.applicationClusterService.localizeApplication(this.localizedResources, this.applicationStateManager);
    }

    @Override
    public void submit(DAG dag, Class... classes) throws IOException {
        if (classes != null) {
            addResource(classes);
        }

        localizeApplication();
        submit0(dag);
    }

    private void submit0(final DAG dag) {
        this.applicationClusterService.submitDag(dag, this.applicationStateManager);
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
    public void addResource(InputStream inputStream,
                            String name,
                            LocalizationResourceType resourceType) throws IOException {
        this.localizedResources.add(new LocalizationResource(inputStream, name, resourceType));
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
    public Map<CounterKey, Accumulator> getAccumulators() {
        return this.applicationClusterService.getAccumulators();
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        return null;
    }
}
