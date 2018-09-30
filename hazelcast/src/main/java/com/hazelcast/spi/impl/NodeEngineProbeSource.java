/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import java.util.Map.Entry;

import com.hazelcast.hotrestart.BackupTaskStatus;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeSource;
import com.hazelcast.internal.metrics.ProbingCycle;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.util.ProbeEnumUtils;

/**
 * {@link ProbeSource} for {@link NodeEngineImpl} that got extracted into a
 * class on its own to not clutter {@link NodeEngineImpl} too much with probing
 * concerns.
 *
 * That means this {@link ProbeSource} is created and registered by the
 * {@link NodeEngineImpl}.
 */
public final class NodeEngineProbeSource implements ProbeSource {

    private final NodeEngineImpl nodeEngine;
    private final OperationServiceImpl operationService;
    private final InternalPartitionServiceImpl partitionService;

    NodeEngineProbeSource(NodeEngineImpl nodeEngine, OperationServiceImpl operationService,
            InternalPartitionServiceImpl partitionService) {
        this.nodeEngine = nodeEngine;
        this.operationService = operationService;
        this.partitionService = partitionService;
    }

    @Override
    public void probeNow(ProbingCycle cycle) {
        cycle.probe("proxy", nodeEngine.getProxyService());
        cycle.probe("operation", operationService);
        cycle.probe("operation", operationService.getInvocationRegistry());
        cycle.probe("operation", operationService.getInboundResponseHandlerSupplier());
        cycle.probe("operation.invocations", operationService.getInvocationMonitor());
        if (cycle.isProbed(ProbeLevel.INFO)) {
            cycle.probe("operation.parker", nodeEngine.getOperationParker());
            cycle.probe("partitions", partitionService);
            cycle.probe("partitions", partitionService.getPartitionStateManager());
            cycle.probe("partitions", partitionService.getMigrationManager());
            cycle.probe("partitions", partitionService.getReplicaManager());
            cycle.probe("transactions", nodeEngine.getTransactionManagerService());
            probeHotRestartStateIn(cycle);
            probeHotBackupStateIn(cycle);
        }
    }

    private void probeHotBackupStateIn(ProbingCycle cycle) {
        HotRestartService hotRestartService = nodeEngine.getNode().getNodeExtension().getHotRestartService();
        boolean enabled = hotRestartService.isHotBackupEnabled();
        cycle.openContext().prefix("hotBackup");
        cycle.gather("enabled", enabled);
        if (enabled) {
            BackupTaskStatus status = hotRestartService.getBackupTaskStatus();
            if (status != null) {
                cycle.gather("state", ProbeEnumUtils.codeOf(status.getState()));
                cycle.gather("completed", status.getCompleted());
                cycle.gather("total", status.getTotal());
            }
        }
    }

    private void probeHotRestartStateIn(ProbingCycle cycle) {
        if (!nodeEngine.getNode().isMaster()) {
            return;
        }
        InternalHotRestartService hotRestartService = nodeEngine.getNode().getNodeExtension()
                .getInternalHotRestartService();
        ClusterHotRestartStatusDTO status = hotRestartService.getCurrentClusterHotRestartStatus();
        if (status != null && status.getHotRestartStatus() != ClusterHotRestartStatus.UNKNOWN) {
            cycle.openContext().prefix("hotRestart");
            cycle.gather("remainingDataLoadTime", status.getRemainingDataLoadTimeMillis());
            cycle.gather("remainingValidationTime", status.getRemainingValidationTimeMillis());
            cycle.gather("status", ProbeEnumUtils.codeOf(status.getHotRestartStatus()));
            cycle.gather("dataRecoveryPolicy", ProbeEnumUtils.codeOf(status.getDataRecoveryPolicy()));
            for (Entry<String, MemberHotRestartStatus> memberStatus : status
                    .getMemberHotRestartStatusMap().entrySet()) {
                cycle.openContext().tag(TAG_INSTANCE, memberStatus.getKey()).prefix("hotRestart");
                cycle.gather("memberStatus", ProbeEnumUtils.codeOf(memberStatus.getValue()));
            }
        }
    }
}
