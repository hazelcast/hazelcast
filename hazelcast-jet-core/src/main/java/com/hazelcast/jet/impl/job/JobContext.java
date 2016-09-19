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

package com.hazelcast.jet.impl.job;


import com.hazelcast.core.IFunction;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.impl.job.deployment.DeploymentStorage;
import com.hazelcast.jet.impl.job.deployment.DiskDeploymentStorage;
import com.hazelcast.jet.impl.runtime.DiscoveryService;
import com.hazelcast.jet.impl.runtime.JobManager;
import com.hazelcast.jet.impl.runtime.task.nio.SocketReader;
import com.hazelcast.jet.impl.runtime.task.nio.SocketWriter;
import com.hazelcast.jet.impl.statemachine.job.JobStateMachine;
import com.hazelcast.jet.impl.statemachine.job.JobStateMachineEventHandler;
import com.hazelcast.jet.runtime.JobListener;
import com.hazelcast.jet.runtime.VertexRunnerListener;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrentReferenceHashMap;
import com.hazelcast.util.IConcurrentMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

public class JobContext {

    private static final IFunction<String, List<VertexRunnerListener>> FUNCTION_FACTORY =
            (IFunction<String, List<VertexRunnerListener>>) input -> new CopyOnWriteArrayList<>();

    private final String name;

    private final NodeEngine nodeEngine;
    private final AtomicReference<Address> owner;
    private final JobManager jobManager;
    private final DeploymentStorage deploymentStorage;
    private final Map<Address, Address> hzToAddressMapping;
    private final JobConfig jobConfig;
    private final JobStateMachine jobStateMachine;
    private final Map<String, Object> jobVariables = new ConcurrentHashMap<>();
    private final List<JobListener> jobListeners = new CopyOnWriteArrayList<>();
    private final IConcurrentMap<String, List<VertexRunnerListener>> vertexRunnerListeners =
            new ConcurrentReferenceHashMap<String, List<VertexRunnerListener>>();

    private final Address localJetAddress;
    private final ExecutorContext executorContext;
    private final Map<Address, SocketWriter> socketWriters = new HashMap<>();
    private final Map<Address, SocketReader> socketReaders = new HashMap<>();
    private final List<Map<String, Accumulator>> accumulators;

    public JobContext(
            String name, NodeEngine nodeEngine, Address localJetAddress, JobConfig jobConfig, JobService jobService
    ) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.localJetAddress = localJetAddress;
        this.owner = new AtomicReference<>();
        this.jobConfig = jobConfig;
        this.executorContext = new ExecutorContext(this.name, this.jobConfig, nodeEngine,
                jobService.getNetworkExecutor(), jobService.getProcessingExecutor());
        this.deploymentStorage = new DiskDeploymentStorage(this, name);
        this.jobStateMachine = new JobStateMachine(name, new JobStateMachineEventHandler(this), this);
        this.hzToAddressMapping = new HashMap<>();
        this.accumulators = new CopyOnWriteArrayList<>();
        this.jobManager = createApplicationMaster(nodeEngine);
    }

    private JobManager createApplicationMaster(NodeEngine nodeEngine) {
        return new JobManager(this,
                new DiscoveryService(this, nodeEngine, socketWriters, socketReaders, hzToAddressMapping));
    }

    /**
     * @param jobOwner owner of job
     * @return true if job was created by node with address specified in @jobOwner
     * false otherwise
     */
    public boolean validateOwner(Address jobOwner) {
        return (owner.compareAndSet(null, jobOwner)) || (owner.compareAndSet(jobOwner, jobOwner));
    }

    /**
     * @return name of the job
     */
    public String getName() {
        return name;
    }

    /**
     * @return node's address which created job
     */
    public Address getOwner() {
        return owner.get();
    }

    /**
     * @return deployment storage for job
     */
    public DeploymentStorage getDeploymentStorage() {
        return deploymentStorage;
    }

    /**
     * @return job's state machine
     */
    public JobStateMachine getJobStateMachine() {
        return jobStateMachine;
    }

    /**
     * @return jobManager
     */
    public JobManager getJobManager() {
        return jobManager;
    }

    /**
     * @return node engine of corresponding to the current node
     */
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    /**
     * @return job's config
     */
    public JobConfig getJobConfig() {
        return jobConfig;
    }

    /**
     * Register life-cycle listener for job
     *
     * @param jobListener corresponding listener
     */
    public void registerJobListener(JobListener jobListener) {
        jobListeners.add(jobListener);
    }

    /**
     * @return all registered vertex runner listeners
     */
    public ConcurrentMap<String, List<VertexRunnerListener>> getVertexRunnerListeners() {
        return vertexRunnerListeners;
    }

    /**
     * @return all registered job listeners
     */
    public List<JobListener> getJobListeners() {
        return jobListeners;
    }

    /**
     * @return direct acyclic graph corresponding to job
     */
    public DAG getDAG() {
        return jobManager.getDag();
    }

    /**
     * @return mapping between main Hazelcast and Jet addresses
     */
    public Map<Address, Address> getHzToJetAddressMapping() {
        return hzToAddressMapping;
    }

    /**
     * @return map of socket Writers
     */
    public Map<Address, SocketWriter> getSocketWriters() {
        return socketWriters;
    }

    /**
     * @return map of socket readers
     */
    public Map<Address, SocketReader> getSocketReaders() {
        return socketReaders;
    }

    /**
     * @return Jet's server address for the current node
     */
    public Address getLocalJetAddress() {
        return localJetAddress;
    }

    /**
     * Register vertex runner listener for the corresponding vertex
     *
     * @param vertexName           name of the corresponding vertex
     * @param vertexRunnerListener vertex runner listener
     */
    public void registerVertexRunnerListener(String vertexName, VertexRunnerListener vertexRunnerListener) {
        List<VertexRunnerListener> listeners = vertexRunnerListeners.applyIfAbsent(vertexName, FUNCTION_FACTORY);
        listeners.add(vertexRunnerListener);
    }

    /**
     * Set up job-local variable
     *
     * @param variableName name of the variable
     * @param variable     value of the variable
     * @param <T>          type of the variable
     */
    public <T> void putJobVariable(String variableName, T variable) {
        jobVariables.put(variableName, variable);
    }

    /**
     * Return the value of the job-local variable
     *
     * @param variableName name of the variable
     * @param <T>          type of the variable
     * @return value of the variable
     */
    @SuppressWarnings("unchecked")
    public <T> T getJobVariable(String variableName) {
        return (T) jobVariables.get(variableName);
    }

    /**
     * Clean job-local variable
     *
     * @param variableName variable name
     */
    public void cleanJobVariable(String variableName) {
        jobVariables.remove(variableName);
    }

    /**
     * @return job Executor context which provides thread-pooling management
     */
    public ExecutorContext getExecutorContext() {
        return executorContext;
    }

    /**
     * @return map with accumulators
     */
    @SuppressWarnings("unchecked")
    public Map<String, Accumulator> getAccumulators() {
        Map<String, Accumulator> map = new HashMap<>();
        for (Map<String, Accumulator> concurrentMap : accumulators) {
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
    public void registerAccumulators(Map<String, Accumulator> accumulatorMap) {
        accumulators.add(accumulatorMap);
    }
}
