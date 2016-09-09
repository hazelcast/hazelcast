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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.executor.BalancedExecutor;
import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.jet.impl.runtime.JobManager;
import com.hazelcast.jet.impl.runtime.jobmanager.JobManagerResponse;
import com.hazelcast.jet.impl.runtime.task.nio.SocketThreadAcceptor;
import com.hazelcast.jet.impl.statemachine.jobmanager.requests.FinalizeJobRequest;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;
import static com.hazelcast.jet.impl.util.JetUtil.uncheckedGet;

public class JobService implements RemoteService {

    public static final int MAX_PORT = 0xFFFF;
    public static final String SERVICE_NAME = "hz:impl:jetService";

    private final Address localJetAddress;
    private final ServerSocketChannel serverSocketChannel;

    private final BalancedExecutor networkExecutor;
    private final BalancedExecutor acceptorExecutor;
    private final BalancedExecutor processingExecutor;

    private final ConcurrentMap<String, JobContext> jobContextMap = new ConcurrentHashMap<>(16);

    private final NodeEngine nodeEngine;

    private final ILogger logger;
    private final JetConfig config;

    public JobService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = this.getNodeEngine().getLogger(JobService.class);
        this.config = JetUtil.resolveDefaultJetConfig(nodeEngine);

        String host = nodeEngine.getLocalMember().getAddress().getHost();
        this.serverSocketChannel = bindSocketChannel(host);
        try {
            this.localJetAddress = new Address(host, this.serverSocketChannel.socket().getLocalPort());
        } catch (UnknownHostException e) {
            throw unchecked(e);
        }

        this.networkExecutor = new BalancedExecutor(
                "network-reader-writer",
                config.getIoThreadCount(),
                config.getShutdownTimeoutSeconds(),
                nodeEngine
        );

        this.processingExecutor = new BalancedExecutor(
                "job_executor",
                config.getProcessingThreadCount(),
                config.getShutdownTimeoutSeconds(),
                nodeEngine
        );

        this.acceptorExecutor = new BalancedExecutor(
                "network-acceptor",
                1,
                config.getShutdownTimeoutSeconds(),
                nodeEngine
        );

        List<Task> taskList = createAcceptorTask(nodeEngine);
        this.acceptorExecutor.submitTaskContext(taskList);

        addShutdownHook(nodeEngine);
    }

    @Override
    public JobProxy createDistributedObject(String objectName) {
        return new JobProxy(objectName, JobService.this, nodeEngine);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        JobContext jobContext = getContext(objectName);
        if (jobContext == null) {
            throw new JetException("No job with name " + objectName + " found.");
        }
        JobManager jobManager = jobContext.getJobManager();
        ICompletableFuture<JobManagerResponse> future = jobManager.handleRequest(new FinalizeJobRequest());
        JobManagerResponse response = uncheckedGet(future);
        if (response.isSuccess()) {
            jobContext.getDeploymentStorage().cleanup();
            jobContext.getExecutorContext().getJobStateMachineExecutor().shutdown();
            jobContext.getExecutorContext().getVertexManagerStateMachineExecutor().shutdown();
            jobContext.getExecutorContext().getJobManagerStateMachineExecutor().shutdown();
            jobContextMap.remove(objectName);
        } else {
            throw new JetException("Unable to finalize job " + objectName);
        }
    }

    private void addShutdownHook(final NodeEngine nodeEngine) {
        nodeEngine.getHazelcastInstance().getLifecycleService().addLifecycleListener(
                new LifecycleListener() {
                    @Override
                    public void stateChanged(LifecycleEvent event) {
                        if (event.getState() == LifecycleEvent.LifecycleState.SHUTTING_DOWN) {
                            try {
                                serverSocketChannel.close();
                                networkExecutor.shutdown();
                                acceptorExecutor.shutdown();
                                processingExecutor.shutdown();
                            } catch (Exception e) {
                                nodeEngine.getLogger(getClass()).warning(e.getMessage(), e);
                            }
                        }
                    }
                }
        );
    }

    private List<Task> createAcceptorTask(NodeEngine nodeEngine) {
        List<Task> taskList = new ArrayList<Task>();
        taskList.add(
                new SocketThreadAcceptor(this, nodeEngine, serverSocketChannel)
        );
        return taskList;
    }

    private ServerSocketChannel bindSocketChannel(String host) {
        try {
            int port = config.getPort();
            while (port <= MAX_PORT) {
                logger.fine("Trying to bind " + host + ":" + port);
                ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
                try {
                    serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                    serverSocketChannel.bind(new InetSocketAddress(host, port));
                    serverSocketChannel.configureBlocking(false);
                    logger.info("Jet is listening on " + host + ":" + port);
                    return serverSocketChannel;
                } catch (java.nio.channels.AlreadyBoundException | java.net.BindException e) {
                    serverSocketChannel.close();
                    if (!config.getNetworkConfig().isPortAutoIncrement()) {
                        break;
                    } else {
                        port++;
                    }
                }
            }
            throw new RuntimeException("Jet was not able to bind to any port");
        } catch (IOException e) {
            throw unchecked(e);
        }
    }

    public JobContext createJobContext(String name, JobConfig config) {
        JobContext jobContext = new JobContext(name, nodeEngine, localJetAddress, config, JobService.this);
        if (jobContextMap.putIfAbsent(name, jobContext) != null) {
            throw new JetException("ApplicationContext for '" + name + " already exists.");
        }
        return jobContext;
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public Address getLocalJetAddress() {
        return localJetAddress;
    }

    public BalancedExecutor getNetworkExecutor() {
        return networkExecutor;
    }

    public BalancedExecutor getProcessingExecutor() {
        return processingExecutor;
    }

    public Collection<JobContext> getJobContextMap() {
        return jobContextMap.values();
    }

    public JobContext getContext(String name) {
        return jobContextMap.get(name);
    }
}
