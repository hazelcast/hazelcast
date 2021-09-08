/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.operation.PrepareForPassiveClusterOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.version.Version;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class JetExtension {

    private final Node node;
    private final ILogger logger;
    private final JetServiceBackend jetServiceBackend;
    private final AtomicBoolean activated = new AtomicBoolean();

    private volatile Version startVersion;

    public JetExtension(Node node, JetServiceBackend jetServiceBackend) {
        this.node = node;
        this.logger = node.getLogger(getClass().getName());
        this.jetServiceBackend = jetServiceBackend;
    }

    private void checkLosslessRestartAllowed() {
        Config config = node.config.getStaticConfig();
        JetConfig jetConfig = config.getJetConfig();
        if (jetConfig.isLosslessRestartEnabled()) {
            if (!BuildInfoProvider.getBuildInfo().isEnterprise()) {
                throw new IllegalStateException("Lossless Restart requires Hazelcast Enterprise Edition");
            }
        }
    }

    public void beforeStart() {
        jetServiceBackend.configureJetInternalObjects(node.config.getStaticConfig(), node.getProperties());

        checkLosslessRestartAllowed();
    }

    public void afterStart() {
        startVersion = node.getClusterService().getClusterVersion();
        if (!tryActivate() && node.isRunning()) {
            logger.info("Jet is disabled due to current cluster version being less than 5.0.");
        }
    }

    /**
     * @return true, if Jet is now active
     */
    private boolean tryActivate() {
        if (activated.get()) {
            // this is a shortcut for the hot path when called for each operation and Jet is already activated.
            return true;
        }
        Version currentVersion = node.getClusterService().getClusterVersion();
        if (node.isRunning()
                && currentVersion.isGreaterOrEqual(Versions.V5_0)
                && activated.compareAndSet(false, true)
        ) {
            jetServiceBackend.getJobCoordinationService().startScanningForJobs();
            if (startVersion != null && !startVersion.equals(currentVersion)) {
                logger.info("Jet is enabled after the cluster version upgrade.");
            } else {
                logger.info("Jet is enabled");
            }
            return true;
        } else {
            return activated.get();
        }
    }

    public void beforeClusterStateChange(ClusterState requestedState) {
        if (!tryActivate() || requestedState != PASSIVE) {
            return;
        }
        logger.info("Jet is preparing to enter the PASSIVE cluster state");
        NodeEngineImpl ne = node.nodeEngine;
        try {
            ne.getOperationService().createInvocationBuilder(JetServiceBackend.SERVICE_NAME,
                    new PrepareForPassiveClusterOperation(), ne.getMasterAddress())
              .invoke().get();
        } catch (InterruptedException | ExecutionException e) {
            throw rethrow(e);
        }
    }

    public void onClusterStateChange() {
        if (tryActivate()) {
            jetServiceBackend.getJobCoordinationService().clusterChangeDone();
        }
    }

    public void onClusterVersionChange() {
        // Activate Jet after rolling upgrade in which the cluster
        // version is upgraded from 4.x to 5.0
        tryActivate();
    }

    public void beforeShutdown(boolean terminate) {
        if (!terminate && tryActivate()) {
            jetServiceBackend.shutDownJobs();
        }
    }

    public void handlePacket(Packet packet) {
        jetServiceBackend.handlePacket(packet);
    }

    public Map<String, Object> createExtensionServices() {
        Map<String, Object> extensionServices = new HashMap<>();

        extensionServices.put(JetServiceBackend.SERVICE_NAME, jetServiceBackend);

        return extensionServices;
    }

    public JetService getJet() {
        if (tryActivate()) {
            return jetServiceBackend.getJet();
        } else {
            throw new IllegalArgumentException("Jet is disabled because the current cluster version is less than 5.0");
        }
    }
}
