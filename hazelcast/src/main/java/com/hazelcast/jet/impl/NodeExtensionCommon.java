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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.operation.PrepareForPassiveClusterOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.sql.impl.JetSqlCoreBackend;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.jet.core.JetProperties.JOB_RESULTS_TTL_SECONDS;
import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static com.hazelcast.jet.impl.JobRepository.JOB_METRICS_MAP_NAME;
import static com.hazelcast.jet.impl.JobRepository.JOB_RESULTS_MAP_NAME;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

class NodeExtensionCommon {
    private static final String JET_LOGO =
            "\to  o   O  o---o o--o o      o-o   O   o-o  o-O-o     o--o  o      O  o-O-o o--o  o-o  o--o  o   o \n" +
            "\t|  |  / \\    /  |    |     /     / \\ |       |       |   | |     / \\   |   |    o   o |   | |\\ /| \n" +
            "\tO--O o---o -O-  O-o  |    O     o---o o-o    |       O--o  |    o---o  |   O-o  |   | O-Oo  | O | \n" +
            "\t|  | |   | /    |    |     \\    |   |    |   |       |     |    |   |  |   |    o   o |  \\  |   | \n" +
            "\to  o o   oo---o o--o O---o  o-o o   oo--o    o       o     O---oo   o  o   o     o-o  o   o o   o";

    private static final String COPYRIGHT_LINE = "Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.";

    private final Node node;
    private final ILogger logger;
    private final JetService jetService;

    NodeExtensionCommon(Node node) {
        this.node = node;
        this.logger = node.getLogger(getClass().getName());
        this.jetService = new JetService(node);
    }

    void beforeStart() {
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
        // TODO we should move this to enterprise node extension
        if (jetConfig.getInstanceConfig().isLosslessRestartEnabled() &&
                !config.getHotRestartPersistenceConfig().isEnabled()) {
            logger.warning("Lossless Restart is enabled but Hot Restart is disabled. Auto-enabling Hot Restart. " +
                    "The following path will be used: " + config.getHotRestartPersistenceConfig().getBaseDir());
            config.getHotRestartPersistenceConfig().setEnabled(true);
        }
    }

    void afterStart() {
        jetService.getJobCoordinationService().startScanningForJobs();
    }

    void beforeClusterStateChange(ClusterState requestedState) {
        if (requestedState != PASSIVE) {
            return;
        }
        logger.info("Jet is preparing to enter the PASSIVE cluster state");
        NodeEngineImpl ne = node.nodeEngine;
        try {
            ne.getOperationService().createInvocationBuilder(JetService.SERVICE_NAME,
                    new PrepareForPassiveClusterOperation(), ne.getMasterAddress())
              .invoke().get();
        } catch (InterruptedException | ExecutionException e) {
            throw rethrow(e);
        }
    }

    void onClusterStateChange(ClusterState ignored) {
        jetService.getJobCoordinationService().clusterChangeDone();
    }

    void beforeShutdown() {
        jetService.shutDownJobs();
    }

    void handlePacket(Packet packet) {
        jetService.handlePacket(packet);
    }

    void printNodeInfo(ILogger log, String addToProductName) {
        log.info(versionAndAddressMessage(addToProductName));
        log.info(imdgVersionMessage());
        log.info(clusterNameMessage());
        log.fine(serializationVersionMessage());
        log.info('\n' + JET_LOGO);
        log.info(COPYRIGHT_LINE);
    }

    private String versionAndAddressMessage(@Nonnull String addToName) {
        JetBuildInfo jetBuildInfo = node.getBuildInfo().getJetBuildInfo();
        String build = jetBuildInfo.getBuild();
        String revision = jetBuildInfo.getRevision();
        if (!revision.isEmpty()) {
            build += " - " + revision;
        }
        return "Hazelcast Jet" + addToName + ' ' + jetBuildInfo.getVersion() +
                " (" + build + ") starting at " + node.getThisAddress();
    }

    private String imdgVersionMessage() {
        String build = node.getBuildInfo().getBuild();
        String revision = node.getBuildInfo().getRevision();
        if (!revision.isEmpty()) {
            build += " - " + revision;
        }
        return "Based on Hazelcast IMDG version: " + node.getVersion() + " (" + build + ")";
    }

    private String serializationVersionMessage() {
        return "Configured Hazelcast Serialization version: " + node.getBuildInfo().getSerializationVersion();
    }

    private String clusterNameMessage() {
        return "Cluster name: " + node.getConfig().getClusterName();
    }

    Map<String, Object> createExtensionServices() {
        Map<String, Object> extensionServices = new HashMap<>();

        extensionServices.put(JetService.SERVICE_NAME, jetService);

        if (jetService.getSqlCoreBackend() != null) {
            extensionServices.put(JetSqlCoreBackend.SERVICE_NAME, jetService.getSqlCoreBackend());
        }

        return extensionServices;
    }

    JetInstance getJetInstance() {
        return jetService.getJetInstance();
    }

}
