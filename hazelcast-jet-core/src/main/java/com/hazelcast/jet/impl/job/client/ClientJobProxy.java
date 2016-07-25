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

package com.hazelcast.jet.impl.job.client;

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.impl.job.JobClusterService;
import com.hazelcast.jet.impl.job.localization.LocalizationResource;
import com.hazelcast.jet.impl.job.localization.LocalizationResourceType;
import com.hazelcast.jet.impl.statemachine.job.JobState;
import com.hazelcast.jet.impl.statemachine.job.JobStateMachine;
import com.hazelcast.jet.impl.util.JetThreadFactory;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.job.Job;

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

public class ClientJobProxy extends ClientProxy implements Job {
    private final Set<LocalizationResource> localizedResources;
    private final JobStateMachine jobStateMachine;
    private JobClusterService jobClusterService;

    public ClientJobProxy(String serviceName,
                          String name) {
        super(serviceName, name);
        localizedResources = new HashSet<>();
        jobStateMachine = new JobStateMachine(name);
    }

    protected void onInitialize() {
        String hzName = getClient().getName();

        ExecutorService executorService = Executors.newCachedThreadPool(
                new JetThreadFactory("client-invoker-job-thread-" + name, hzName)
        );

        jobClusterService = new ClientJobClusterService(
                getClient(),
                name,
                executorService
        );
    }

    public void init(JobConfig config) {
        if (config == null) {
            config = JetUtil.resolveJobConfig(getClient(), name);
        }
        jobClusterService.init(config, jobStateMachine);
    }

    private void localizeApplication() {
        jobClusterService.localize(localizedResources, jobStateMachine);
    }

    @Override
    public void submit(DAG dag, Class... classes) {
        if (classes != null) {
            try {
                addResource(classes);
            } catch (IOException e) {
                throw JetUtil.reThrow(e);
            }
        }

        localizeApplication();
        submit0(dag);
    }

    private void submit0(final DAG dag) {
        jobClusterService.submitDag(dag, jobStateMachine);
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
    public JobState getJobState() {
        return jobStateMachine.currentState();
    }

    @Override
    public Future execute() {
        return jobClusterService.execute(jobStateMachine);
    }

    @Override
    public Future interrupt() {
        return jobClusterService.interrupt(jobStateMachine);
    }

    @Override
    protected boolean preDestroy() {
        try {
            jobClusterService.destroy(jobStateMachine).get();
            return true;
        } catch (Exception e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public Map<String, Accumulator> getAccumulators() {
        return jobClusterService.getAccumulators();
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        return null;
    }
}
