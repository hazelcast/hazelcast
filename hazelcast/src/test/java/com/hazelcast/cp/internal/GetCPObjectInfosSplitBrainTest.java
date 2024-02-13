/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPObjectInfo;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.cp.CPSubsystem.ATOMIC_LONG;
import static com.hazelcast.cp.internal.HazelcastRaftTestSupport.waitAllForLeaderElection;
import static com.hazelcast.cp.internal.HazelcastRaftTestSupport.waitUntilCPDiscoveryCompleted;
import static com.hazelcast.spi.properties.ClusterProperty.MAX_NO_HEARTBEAT_SECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class GetCPObjectInfosSplitBrainTest extends RaftSplitBrainTestSupport {

    @Override
    protected Config config() {
        Config config = super.config();
        config.getCPSubsystemConfig().getRaftAlgorithmConfig().setLeaderHeartbeatPeriodInMillis(SECONDS.toMillis(3));
        config.getCPSubsystemConfig().getRaftAlgorithmConfig().setMaxMissedLeaderHeartbeatCount(2);
        MAX_NO_HEARTBEAT_SECONDS.setSystemProperty("5");
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitUntilCPDiscoveryCompleted(instances);

        instances[0].getCPSubsystem().getAtomicLong("long").set(1);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        sleepSeconds(10);
        CPGroupId groupId = groupId(firstBrain[0], "default");

        // firstBrain is the majority, the call should succeed there
        for (HazelcastInstance instance : firstBrain) {
            Iterable<CPObjectInfo> infos = instance.getCPSubsystem().getObjectInfos(groupId, ATOMIC_LONG);
            assertThat(infos)
                    .extracting(CPObjectInfo::name, CPObjectInfo::serviceName, CPObjectInfo::groupId)
                    .containsExactly(tuple("long", ATOMIC_LONG, groupId));
        }

        // The call on minority should result in an exception
        for (HazelcastInstance instance : secondBrain) {
            assertThatThrownBy(() ->
                    instance.getCPSubsystem().getObjectInfos(groupId, ATOMIC_LONG)
            ).isInstanceOf(NotLeaderException.class);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        CPGroupId groupId = groupId(instances[4], "default");
        waitAllForLeaderElection(instances, groupId);

        Iterable<CPObjectInfo> objectInfos = instances[4].getCPSubsystem().getObjectInfos(groupId, ATOMIC_LONG);
        assertThat(objectInfos)
                .extracting(CPObjectInfo::name, CPObjectInfo::serviceName, CPObjectInfo::groupId)
                .containsExactly(tuple("long", ATOMIC_LONG, groupId));
    }

    CPGroupId groupId(HazelcastInstance instance, String name) {
        try {
            return instance.getCPSubsystem()
                    .getCPSubsystemManagementService()
                    .getCPGroup(name)
                    .toCompletableFuture()
                    .get().id();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
