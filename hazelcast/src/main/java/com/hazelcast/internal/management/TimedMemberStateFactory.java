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

package com.hazelcast.internal.management;

import com.hazelcast.config.GroupConfig;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.monitor.LocalOperationStats;
import com.hazelcast.monitor.impl.HotRestartStateImpl;
import com.hazelcast.monitor.impl.LocalMemoryStatsImpl;
import com.hazelcast.monitor.impl.LocalOperationStatsImpl;
import com.hazelcast.monitor.impl.MemberPartitionStateImpl;
import com.hazelcast.monitor.impl.MemberStateImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.partition.IPartition;
import java.util.List;

/**
 * A Factory for creating {@link TimedMemberState} instances.
 */
public class TimedMemberStateFactory {

    protected final HazelcastInstanceImpl instance;

    public TimedMemberStateFactory(HazelcastInstanceImpl instance) {
        this.instance = instance;

        if (instance.node.getProperties().get("hazelcast.mc.max.visible.instance.count") != null) {
            instance.node.loggingService.getLogger(getClass())
                    .warning("hazelcast.mc.max.visible.instance.count property is removed.");
        }
    }

    public TimedMemberState createTimedMemberState() {
        MemberStateImpl memberState = new MemberStateImpl();

        TimedMemberState timedMemberState = new TimedMemberState();
        createMemberState(memberState);
        timedMemberState.setMemberState(memberState);
        GroupConfig groupConfig = instance.getConfig().getGroupConfig();
        timedMemberState.setClusterName(groupConfig.getName());

        return timedMemberState;
    }

    protected LocalMemoryStats getMemoryStats() {
        return new LocalMemoryStatsImpl(instance.getMemoryStats());
    }

    protected LocalOperationStats getOperationStats() {
        return new LocalOperationStatsImpl(instance.node);
    }

    private void createMemberState(MemberStateImpl memberState) {
        Node node = instance.node;

        Address thisAddress = node.getThisAddress();
        memberState.setAddress(thisAddress.getHost() + ":" + thisAddress.getPort());

        MemberPartitionStateImpl memberPartitionState = (MemberPartitionStateImpl) memberState.getMemberPartitionState();
        InternalPartitionService partitionService = node.getPartitionService();
        IPartition[] partitions = partitionService.getPartitions();

        List<Integer> partitionList = memberPartitionState.getPartitions();
        for (IPartition partition : partitions) {
            if (partition.isLocal()) {
                partitionList.add(partition.getPartitionId());
            }
        }

        memberState.setOperationStats(getOperationStats());
        createHotRestartState(memberState);
    }

    private void createHotRestartState(MemberStateImpl memberState) {
        final HotRestartService hotRestartService = instance.node.getNodeExtension().getHotRestartService();
        memberState.setHotRestartState(new HotRestartStateImpl(hotRestartService.getBackupDirectory()));
    }
}
