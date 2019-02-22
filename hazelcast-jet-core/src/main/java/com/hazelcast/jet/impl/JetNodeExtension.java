/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.Node;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.NodeEngineImpl.JetPacketConsumer;

import java.util.Map;

public class JetNodeExtension extends DefaultNodeExtension implements JetPacketConsumer {
    private final NodeExtensionCommon extCommon;

    public JetNodeExtension(Node node) {
        super(node);
        extCommon = new NodeExtensionCommon(node, new JetService(node));
    }

    @Override
    public void beforeStart() {
        JetConfig config = JetService.findJetServiceConfig(node.getConfig());
        if (config.getInstanceConfig().isLosslessRestartEnabled()) {
            throw new UnsupportedOperationException("Lossless Restart is not available in the open-source version of " +
                    "Hazelcast Jet");
        }
        super.beforeStart();
    }

    @Override
    public void afterStart() {
        super.afterStart();
        extCommon.afterStart();
    }

    @Override
    public void beforeClusterStateChange(ClusterState currState, ClusterState requestedState, boolean isTransient) {
        super.beforeClusterStateChange(currState, requestedState, isTransient);
        extCommon.beforeClusterStateChange(requestedState);
    }

    @Override
    public void onClusterStateChange(ClusterState newState, boolean isTransient) {
        super.onClusterStateChange(newState, isTransient);
        extCommon.onClusterStateChange(newState);
    }

    @Override
    public Map<String, Object> createExtensionServices() {
        return extCommon.createExtensionServices();
    }

    @Override
    public void printNodeInfo() {
        extCommon.printNodeInfo(systemLogger, "");
    }

    @Override
    public void accept(Packet packet) {
        extCommon.handlePacket(packet);
    }
}
