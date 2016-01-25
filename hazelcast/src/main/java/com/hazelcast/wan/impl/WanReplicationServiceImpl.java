/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan.impl;

import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.wan.WanReplicationEndpoint;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Open source implementation of the {@link com.hazelcast.wan.WanReplicationService}
 */
public class WanReplicationServiceImpl implements WanReplicationService {

    private final Node node;
    private final ILogger logger;

    private final Map<String, WanReplicationPublisherDelegate> wanReplications = initializeWanReplicationPublisherMapping();
    private final Object mutex = new Object();
    private volatile StripedExecutor executor;

    public WanReplicationServiceImpl(Node node) {
        this.node = node;
        this.logger = node.getLogger(WanReplicationServiceImpl.class.getName());
    }

    @Override
    @SuppressWarnings("SynchronizeOnThis")
    public WanReplicationPublisher getWanReplicationPublisher(String name) {
        WanReplicationPublisherDelegate wr = wanReplications.get(name);
        if (wr != null) {
            return wr;
        }
        synchronized (this) {
            wr = wanReplications.get(name);
            if (wr != null) {
                return wr;
            }
            WanReplicationConfig wanReplicationConfig = node.getConfig().getWanReplicationConfig(name);
            if (wanReplicationConfig == null) {
                return null;
            }
            List<WanTargetClusterConfig> targets = wanReplicationConfig.getTargetClusterConfigs();
            WanReplicationEndpoint[] targetEndpoints = new WanReplicationEndpoint[targets.size()];
            int count = 0;
            for (WanTargetClusterConfig targetClusterConfig : targets) {
                WanReplicationEndpoint target;
                try {
                    if (targetClusterConfig.getReplicationImplObject() != null) {
                        target = (WanReplicationEndpoint) targetClusterConfig.getReplicationImplObject();
                    } else {
                        target = ClassLoaderUtil
                                .newInstance(node.getConfigClassLoader(), targetClusterConfig.getReplicationImpl());
                    }
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
                String groupName = targetClusterConfig.getGroupName();
                String password = targetClusterConfig.getGroupPassword();
                String[] addresses = new String[targetClusterConfig.getEndpoints().size()];
                targetClusterConfig.getEndpoints().toArray(addresses);
                target.init(node, groupName, password, addresses);
                targetEndpoints[count++] = target;
            }
            wr = new WanReplicationPublisherDelegate(name, targetEndpoints);
            wanReplications.put(name, wr);
            return wr;
        }
    }

    @Override
    public void handle(final Packet packet) {
        StripedExecutor ex = getExecutor();
        ex.execute(new StripedRunnable() {
            @Override
            public void run() {
                try {
                    WanReplicationEvent replicationEvent = (WanReplicationEvent) node.nodeEngine.toObject(packet);
                    String serviceName = replicationEvent.getServiceName();
                    ReplicationSupportingService service = node.nodeEngine.getService(serviceName);
                    service.onReplicationEvent(replicationEvent);
                } catch (Exception e) {
                    logger.severe(e);
                }
            }

            @Override
            public int getKey() {
                return packet.getPartitionId();
            }
        });
    }

    private StripedExecutor getExecutor() {
        StripedExecutor ex = executor;
        if (ex == null) {
            synchronized (mutex) {
                if (executor == null) {
                    HazelcastThreadGroup threadGroup = node.getHazelcastThreadGroup();
                    executor = new StripedExecutor(node.getLogger(WanReplicationServiceImpl.class),
                            threadGroup.getThreadNamePrefix("wan"),
                            threadGroup.getInternalThreadGroup(),
                            ExecutorConfig.DEFAULT_POOL_SIZE,
                            ExecutorConfig.DEFAULT_QUEUE_CAPACITY);
                }
                ex = executor;
            }
        }
        return ex;
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            for (WanReplicationPublisherDelegate wanReplication : wanReplications.values()) {
                WanReplicationEndpoint[] wanReplicationEndpoints = wanReplication.getEndpoints();
                if (wanReplicationEndpoints != null) {
                    for (WanReplicationEndpoint wanReplicationEndpoint : wanReplicationEndpoints) {
                        if (wanReplicationEndpoint != null) {
                            wanReplicationEndpoint.shutdown();
                        }
                    }
                }
            }
            StripedExecutor ex = executor;
            if (ex != null) {
                ex.shutdown();
            }
            wanReplications.clear();
        }
    }

    @Override
    public void pause(String name, String targetGroupName) {
        throw new UnsupportedOperationException("Pausing wan replication is not supported.");
    }

    @Override
    public void resume(String name, String targetGroupName) {
        throw new UnsupportedOperationException("Resuming wan replication is not supported");
    }

    @Override
    public void checkWanReplicationQueues(String name) {
        //NOP
    }

    private ConcurrentHashMap<String, WanReplicationPublisherDelegate> initializeWanReplicationPublisherMapping() {
        return new ConcurrentHashMap<String, WanReplicationPublisherDelegate>(2);
    }

    @Override
    public Map<String, LocalWanStats> getStats() {
        return null;
    }
}
