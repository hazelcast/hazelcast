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

import com.hazelcast.core.IFunction;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.jet.internal.api.JetApplicationManager;
import com.hazelcast.jet.internal.api.application.ApplicationContext;
import com.hazelcast.jet.internal.api.executor.SharedApplicationExecutor;
import com.hazelcast.jet.internal.api.executor.Task;
import com.hazelcast.jet.internal.impl.container.task.nio.DefaultSocketThreadAcceptor;
import com.hazelcast.jet.internal.impl.executor.DefaultApplicationTaskContext;
import com.hazelcast.jet.internal.impl.executor.SharedBalancedExecutorImpl;
import com.hazelcast.jet.internal.impl.util.JetUtil;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.IConcurrentMap;
import com.hazelcast.util.SampleableConcurrentHashMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class JetApplicationManagerImpl implements JetApplicationManager {
    public static final int MAX_PORT = 0xFFFF;
    private final NodeEngine nodeEngine;
    private final Address localJetAddress;
    private final ServerSocketChannel serverSocketChannel;
    private final SharedApplicationExecutor networkExecutor;
    private final SharedApplicationExecutor acceptorExecutor;

    private final SharedApplicationExecutor processingExecutor;

    private final ThreadLocal<JetApplicationConfig> threadLocal = new ThreadLocal<JetApplicationConfig>();

    private final IConcurrentMap<String, ApplicationContext> applicationContexts =
            new SampleableConcurrentHashMap<String, ApplicationContext>(16);

    private final IFunction<String, ApplicationContext> function = new IFunction<String, ApplicationContext>() {
        @Override
        public ApplicationContext apply(String name) {
            return new ApplicationContextImpl(
                    name,
                    nodeEngine,
                    localJetAddress,
                    threadLocal.get(),
                    JetApplicationManagerImpl.this
            );
        }
    };
    private final ILogger logger;

    public JetApplicationManagerImpl(final NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = this.getNodeEngine().getLogger(JetApplicationManager.class);

        try {
            String host = nodeEngine.getLocalMember().getAddress().getHost();
            this.serverSocketChannel = bindSocketChannel(host);
            this.localJetAddress = new Address(host, this.serverSocketChannel.socket().getLocalPort());

            JetApplicationConfig defaultJetConfig =
                    JetUtil.resolveJetDefaultApplicationConfig(nodeEngine);

            this.networkExecutor = new SharedBalancedExecutorImpl(
                    "network-reader-writer",
                    defaultJetConfig.getIoThreadCount(),
                    defaultJetConfig.getJetSecondsToAwait(),
                    nodeEngine
            );

            this.processingExecutor = new SharedBalancedExecutorImpl(
                    "application_executor",
                    defaultJetConfig.getMaxProcessingThreads(),
                    defaultJetConfig.getJetSecondsToAwait(),
                    nodeEngine
            );

            this.acceptorExecutor = new SharedBalancedExecutorImpl(
                    "network-acceptor",
                    1,
                    defaultJetConfig.getJetSecondsToAwait(),
                    nodeEngine
            );

            List<Task> taskList = createAcceptorTask(nodeEngine);
            this.acceptorExecutor.submitTaskContext(new DefaultApplicationTaskContext(
                    taskList
            ));

            addShutdownHook(nodeEngine);
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    private void addShutdownHook(final NodeEngine nodeEngine) {
        nodeEngine.getHazelcastInstance().getLifecycleService().addLifecycleListener(
                new LifecycleListener() {
                    @Override
                    public void stateChanged(LifecycleEvent event) {
                        if (event.getState() == LifecycleEvent.LifecycleState.SHUTTING_DOWN) {
                            try {
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
                new DefaultSocketThreadAcceptor(
                        this,
                        nodeEngine,
                        this.serverSocketChannel
                )
        );
        return taskList;
    }

    private ServerSocketChannel bindSocketChannel(String host) {
        try {
            int port = JetApplicationConfig.DEFAULT_PORT;
            while (port <= MAX_PORT) {
                logger.info("Trying to bind " + host + ":" + port);
                ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
                try {
                    serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                    serverSocketChannel.bind(new InetSocketAddress(host, port));
                    serverSocketChannel.configureBlocking(false);
                    return serverSocketChannel;
                } catch (java.nio.channels.AlreadyBoundException | java.net.BindException e) {
                    serverSocketChannel.close();
                    port += JetApplicationConfig.PORT_AUTO_INCREMENT;
                }
            }
            throw new RuntimeException("Jet was not able to bind to any port");
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public ApplicationContext createOrGetApplicationContext(String name,
                                                            JetApplicationConfig config) {
        this.threadLocal.set(config);

        try {
            return this.applicationContexts.applyIfAbsent(
                    name,
                    this.function
            );
        } finally {
            this.threadLocal.set(null);
            wakeUp();
        }
    }

    @Override
    public NodeEngine getNodeEngine() {
        return this.nodeEngine;
    }

    private void wakeUp() {
        this.networkExecutor.wakeUp();
        this.acceptorExecutor.wakeUp();
        this.processingExecutor.wakeUp();
    }

    @Override
    public void destroyApplication(String name) {
        this.applicationContexts.remove(name);
    }

    @Override
    public Address getLocalJetAddress() {
        return this.localJetAddress;
    }

    @Override
    public SharedApplicationExecutor getNetworkExecutor() {
        return this.networkExecutor;
    }

    @Override
    public SharedApplicationExecutor getProcessingExecutor() {
        return this.processingExecutor;
    }

    @Override
    public Collection<ApplicationContext> getApplicationContexts() {
        return this.applicationContexts.values();
    }

    @Override
    public SharedApplicationExecutor getAcceptorExecutor() {
        return this.acceptorExecutor;
    }

    @Override
    public ApplicationContext getApplicationContext(String name) {
        return this.applicationContexts.get(name);
    }
}
