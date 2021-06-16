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
import com.hazelcast.config.MapConfig;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.operation.PrepareForPassiveClusterOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.sql.impl.JetSqlCoreBackend;
import com.hazelcast.version.Version;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.jet.core.JetProperties.JOB_RESULTS_TTL_SECONDS;
import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static com.hazelcast.jet.impl.JobRepository.JOB_METRICS_MAP_NAME;
import static com.hazelcast.jet.impl.JobRepository.JOB_RESULTS_MAP_NAME;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class JetExtension {

    private final Node node;
    private final ILogger logger;
    private final JetServiceBackend jetServiceBackend;
    private volatile boolean activated;
    private volatile boolean isAfterStartCalled;

    public JetExtension(Node node, JetServiceBackend jetServiceBackend) {
        this.node = node;
        this.logger = node.getLogger(getClass().getName());
        this.jetServiceBackend = jetServiceBackend;
        this.activated = false;
        this.isAfterStartCalled = false;
    }

    private void checkLosslessRestartAllowed() {
        Config config = node.config.getStaticConfig();
        JetConfig jetConfig = config.getJetConfig();
        if (jetConfig.getInstanceConfig().isLosslessRestartEnabled()) {
            if (!BuildInfoProvider.getBuildInfo().isEnterprise()) {
                throw new IllegalStateException("Lossless Restart requires Hazelcast Enterprise Edition");
            }
        }
    }

    public void beforeStart() {
        Config config = node.config.getStaticConfig();
        JetConfig jetConfig = config.getJetConfig();

        MapConfig internalMapConfig = new MapConfig(INTERNAL_JET_OBJECTS_PREFIX + '*')
                .setBackupCount(jetConfig.getInstanceConfig().getBackupCount())
                // we query creationTime of resources maps
                .setStatisticsEnabled(true);

        internalMapConfig.getMergePolicyConfig().setPolicy(DiscardMergePolicy.class.getName());

        MapConfig resultsMapConfig = new MapConfig(internalMapConfig)
                .setName(JOB_RESULTS_MAP_NAME)
                .setTimeToLiveSeconds(node.getProperties().getSeconds(JOB_RESULTS_TTL_SECONDS));

        MapConfig metricsMapConfig = new MapConfig(internalMapConfig)
                .setName(JOB_METRICS_MAP_NAME)
                .setTimeToLiveSeconds(node.getProperties().getSeconds(JOB_RESULTS_TTL_SECONDS));

        config.addMapConfig(internalMapConfig)
                .addMapConfig(resultsMapConfig)
                .addMapConfig(metricsMapConfig);

        checkLosslessRestartAllowed();
    }

    public void afterStart() {
        if (node.isRunning() && node.getClusterService().getClusterVersion().isGreaterOrEqual(Versions.V5_0)) {
            activated = true;
            jetServiceBackend.getJobCoordinationService().startScanningForJobs();
            logger.info("Jet is enabled");
        } else {
            logger.info("Jet is disabled due to current cluster version being less than 5.0.");
        }
        isAfterStartCalled = true;
    }

    public void beforeClusterStateChange(ClusterState requestedState) {
        if (!activated || requestedState != PASSIVE) {
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

    public void onClusterStateChange(ClusterState ignored) {
        if (activated) {
            jetServiceBackend.getJobCoordinationService().clusterChangeDone();
        }
    }

    public void onClusterVersionChange(Version newVersion) {
        if (!activated && isAfterStartCalled && newVersion.isGreaterOrEqual(Versions.V5_0)) {
            // Activate Jet after rolling upgrade in which the cluster
            // version is upgraded from 4.x to 5.0
            activated = true;
            jetServiceBackend.getJobCoordinationService().startScanningForJobs();
            logger.info("Jet is enabled after the cluster version upgrade.");
        }
    }

    public void beforeShutdown(boolean terminate) {
        if (!terminate && activated) {
            jetServiceBackend.shutDownJobs();
        }
    }

    public void handlePacket(Packet packet) {
        jetServiceBackend.handlePacket(packet);
    }

    public Map<String, Object> createExtensionServices() {
        Map<String, Object> extensionServices = new HashMap<>();

        extensionServices.put(JetServiceBackend.SERVICE_NAME, jetServiceBackend);

        if (jetServiceBackend.getSqlCoreBackend() != null) {
            extensionServices.put(JetSqlCoreBackend.SERVICE_NAME, jetServiceBackend.getSqlCoreBackend());
        }

        return extensionServices;
    }

    public JetService getJet() {
        if (activated) {
            return jetServiceBackend.getJet();
        } else {
            throw new IllegalArgumentException("Jet is disabled because the current cluster version is less than 5.0");
        }
    }

}
