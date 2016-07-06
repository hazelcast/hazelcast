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

package com.hazelcast.jet.impl.application.client;

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.application.Application;
import com.hazelcast.jet.config.ApplicationConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.impl.application.ApplicationClusterService;
import com.hazelcast.jet.impl.application.LocalizationResource;
import com.hazelcast.jet.impl.application.LocalizationResourceType;
import com.hazelcast.jet.impl.statemachine.application.ApplicationState;
import com.hazelcast.jet.impl.statemachine.application.ApplicationStateMachine;
import com.hazelcast.jet.impl.util.JetThreadFactory;
import com.hazelcast.jet.impl.util.JetUtil;

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

public class ClientApplicationProxy extends ClientProxy implements Application {
    private final Set<LocalizationResource> localizedResources;
    private final ApplicationStateMachine applicationStateMachine;
    private ApplicationClusterService applicationClusterService;

    public ClientApplicationProxy(String serviceName,
                                  String name) {
        super(serviceName, name);
        localizedResources = new HashSet<>();
        applicationStateMachine = new ApplicationStateMachine(name);
    }

    protected void onInitialize() {
        String hzName = getClient().getName();

        ExecutorService executorService = Executors.newCachedThreadPool(
                new JetThreadFactory("client-invoker-application-thread-" + name, hzName)
        );

        applicationClusterService = new ClientApplicationClusterService(
                getClient(),
                name,
                executorService
        );
    }

    public void init(ApplicationConfig config) {
        if (config == null) {
            config = JetUtil.resolveApplicationConfig(getClient(), name);
        }
        applicationClusterService.init(config, applicationStateMachine);
    }

    private void localizeApplication() {
        applicationClusterService.localize(localizedResources, applicationStateMachine);
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
        applicationClusterService.submitDag(dag, applicationStateMachine);
    }

    @Override
    public void addResource(Class... classes) throws IOException {
        checkNotNull(classes, "Classes can not be null");

        for (Class clazz : classes) {
            localizedResources.add(new LocalizationResource(clazz));
        }
    }

    @Override
    public void addResource(URL url) throws IOException {
        localizedResources.add(new LocalizationResource(url));
    }

    @Override
    public void addResource(InputStream inputStream,
                            String name,
                            LocalizationResourceType resourceType) throws IOException {
        localizedResources.add(new LocalizationResource(inputStream, name, resourceType));
    }

    @Override
    public void clearResources() {
        localizedResources.clear();
    }

    @Override
    public ApplicationState getApplicationState() {
        return applicationStateMachine.currentState();
    }

    @Override
    public Future execute() {
        return applicationClusterService.execute(applicationStateMachine);
    }

    @Override
    public Future interrupt() {
        return applicationClusterService.interrupt(applicationStateMachine);
    }

    @Override
    protected boolean preDestroy() {
        try {
            applicationClusterService.destroy(applicationStateMachine).get();
            return true;
        } catch (Exception e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public Map<String, Accumulator> getAccumulators() {
        return applicationClusterService.getAccumulators();
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        return null;
    }
}
