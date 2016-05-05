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

package com.hazelcast.wan.impl;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationEndpoint;
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
            List<WanPublisherConfig> publisherConfigs = wanReplicationConfig.getWanPublisherConfigs();
            WanReplicationEndpoint[] targetEndpoints = new WanReplicationEndpoint[publisherConfigs.size()];
            int count = 0;
            for (WanPublisherConfig publisherConfig : publisherConfigs) {
                WanReplicationEndpoint target;
                try {
                    if (publisherConfig.getImplementation() != null) {
                        target = (WanReplicationEndpoint) publisherConfig.getImplementation();
                    } else {
                        target = ClassLoaderUtil
                                .newInstance(node.getConfigClassLoader(), publisherConfig.getClassName());
                    }
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
                target.init(node, wanReplicationConfig, publisherConfig);
                targetEndpoints[count++] = target;
            }
            wr = new WanReplicationPublisherDelegate(name, targetEndpoints);
            wanReplications.put(name, wr);
            return wr;
        }
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

    @Override
    public void syncMap(String wanReplicationName, String targetGroupName, String mapName) {
        throw new UnsupportedOperationException("WAN sync for map is not supported.");
    }

    private ConcurrentHashMap<String, WanReplicationPublisherDelegate> initializeWanReplicationPublisherMapping() {
        return new ConcurrentHashMap<String, WanReplicationPublisherDelegate>(2);
    }

    @Override
    public Map<String, LocalWanStats> getStats() {
        return null;
    }
}
