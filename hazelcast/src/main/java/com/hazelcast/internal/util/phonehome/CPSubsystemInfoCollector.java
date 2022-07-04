/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.cp.internal.datastructures.lock.LockService;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.instance.impl.Node;

import java.util.function.BiConsumer;

public class CPSubsystemInfoCollector implements MetricsCollector {

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        int cpMemberCount = node.getNodeEngine().getConfig().getCPSubsystemConfig().getCPMemberCount();
        boolean cpSubsystemEnabled = cpMemberCount != 0;
        metricsConsumer.accept(PhoneHomeMetrics.CP_SUBSYSTEM_ENABLED, String.valueOf(cpSubsystemEnabled));
        if (cpSubsystemEnabled) {
            metricsConsumer.accept(PhoneHomeMetrics.CP_MEMBERS_COUNT, String.valueOf(cpMemberCount));

            RaftService raftService = node.getNodeEngine().getService(RaftService.SERVICE_NAME);
            int groupsCount = raftService.getMetadataGroupManager().getGroupIds().size();
            metricsConsumer.accept(PhoneHomeMetrics.CP_GROUPS_COUNT, String.valueOf(groupsCount));

            SemaphoreService semaphoreService = node.getNodeEngine().getService(SemaphoreService.SERVICE_NAME);
            int semaphoresCount = semaphoreService.getTotalResourcesCount();
            metricsConsumer.accept(PhoneHomeMetrics.CP_SEMAPHORES_COUNT, String.valueOf(semaphoresCount));

            CountDownLatchService clService = node.getNodeEngine().getService(CountDownLatchService.SERVICE_NAME);
            int clCount = clService.getTotalResourcesCount();
            metricsConsumer.accept(PhoneHomeMetrics.CP_COUNTDOWN_LATCHES_COUNT, String.valueOf(clCount));

            LockService lockService = node.getNodeEngine().getService(LockService.SERVICE_NAME);
            int locksCount = lockService.getTotalResourcesCount();
            metricsConsumer.accept(PhoneHomeMetrics.CP_FENCED_LOCKS_COUNT, String.valueOf(locksCount));

            AtomicLongService atomicLongService = node.getNodeEngine().getService(AtomicLongService.SERVICE_NAME);
            int atomicLongsCount = atomicLongService.getAtomicValuesCount();
            metricsConsumer.accept(PhoneHomeMetrics.CP_ATOMIC_LONGS_COUNT, String.valueOf(atomicLongsCount));

            AtomicRefService atomicRefService = node.getNodeEngine().getService(AtomicRefService.SERVICE_NAME);
            int atomicRefsCount = atomicRefService.getAtomicValuesCount();
            metricsConsumer.accept(PhoneHomeMetrics.CP_ATOMIC_REFS_COUNT, String.valueOf(atomicRefsCount));
        }
    }
}
