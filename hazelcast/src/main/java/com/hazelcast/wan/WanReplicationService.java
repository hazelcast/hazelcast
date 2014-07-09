/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan;

import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.impl.ExecutionServiceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WanReplicationService implements CoreService {

    public static final String SERVICE_NAME = "hz:core:wanReplicationService";

    private final Node node;
    private final ILogger logger;

    private final Map<String, WanReplicationDelegate> wanReplications = new ConcurrentHashMap<String, WanReplicationDelegate>(2);
    private final StripedExecutor executor;

    public WanReplicationService(NodeEngineImpl nodeEngine) {
        this.node = nodeEngine.getNode();
        this.logger = node.getLogger(WanReplicationService.class.getName());
        final ExecutionServiceImpl executionService = (ExecutionServiceImpl) nodeEngine.getExecutionService();
        this.executor = new StripedExecutor(
                executionService.getCachedExecutor(),
                ExecutorConfig.DEFAULT_POOL_SIZE,
                ExecutorConfig.DEFAULT_QUEUE_CAPACITY
        );
    }

    @SuppressWarnings("SynchronizeOnThis")
    public WanReplicationPublisher getWanReplicationListener(String name) {
        WanReplicationDelegate wr = wanReplications.get(name);
        if (wr != null) return wr;
        synchronized (this) {
            wr = wanReplications.get(name);
            if (wr != null) return wr;
            WanReplicationConfig wanReplicationConfig = node.getConfig().getWanReplicationConfig(name);
            if (wanReplicationConfig == null) return null;
            List<WanTargetClusterConfig> targets = wanReplicationConfig.getTargetClusterConfigs();
            WanReplicationEndpoint[] targetEndpoints = new WanReplicationEndpoint[targets.size()];
            int count = 0;
            for (WanTargetClusterConfig targetClusterConfig : targets) {
                WanReplicationEndpoint target;
                if (targetClusterConfig.getReplicationImpl() != null) {
                    try {
                        target = ClassLoaderUtil.newInstance(node.getConfigClassLoader(), targetClusterConfig.getReplicationImpl());
                    } catch (Exception e) {
                        throw ExceptionUtil.rethrow(e);
                    }
                } else {
                    target = new WanNoDelayReplication();
                }
                String groupName = targetClusterConfig.getGroupName();
                String password = targetClusterConfig.getGroupPassword();
                String[] addresses = new String[targetClusterConfig.getEndpoints().size()];
                targetClusterConfig.getEndpoints().toArray(addresses);
                target.init(node, groupName, password, addresses);
                targetEndpoints[count++] = target;
            }
            wr = new WanReplicationDelegate(name, targetEndpoints);
            wanReplications.put(name, wr);
            return wr;
        }
    }

    public void handleEvent(final Packet packet) {
        executor.execute(new StripedRunnable() {
            @Override
            public void run() {
                final Data data = packet.getData();
                try {
                    WanReplicationEvent replicationEvent = (WanReplicationEvent) node.nodeEngine.toObject(data);
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

    public void shutdown() {
        synchronized (this) {
            for (WanReplicationDelegate wanReplication : wanReplications.values()) {
                WanReplicationEndpoint[] wanReplicationEndpoints = wanReplication.getEndpoints();
                if (wanReplicationEndpoints != null) {
                    for (WanReplicationEndpoint wanReplicationEndpoint : wanReplicationEndpoints) {
                        if (wanReplicationEndpoint != null) {
                            wanReplicationEndpoint.shutdown();
                        }
                    }
                }
            }
            wanReplications.clear();
            executor.shutdown();
        }
    }
}
