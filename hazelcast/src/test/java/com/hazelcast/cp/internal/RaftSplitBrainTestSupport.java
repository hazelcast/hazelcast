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

package com.hazelcast.cp.internal;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.SplitBrainTestSupport;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

public abstract class RaftSplitBrainTestSupport extends SplitBrainTestSupport {

    @Parameters(name = "withSessionTimeout:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{true, false});
    }

    @Parameterized.Parameter
    public boolean withSessionTimeout;

    @Override
    protected int[] brains() {
        return new int[]{3, 2};
    }

    @Override
    protected Config config() {
        Config config = super.config();
        config.getCPSubsystemConfig()
                .setCPMemberCount(getCPMemberCount())
                .setGroupSize(getGroupSize())
                .setFailOnIndeterminateOperationState(true);
        if (withSessionTimeout) {
            config.getCPSubsystemConfig()
                    .setSessionHeartbeatIntervalSeconds(1)
                    .setSessionTimeToLiveSeconds(5);
        }
        return config;
    }

    protected int getGroupSize() {
        return getCPMemberCount();
    }

    protected int getCPMemberCount() {
        int size = 0;
        for (int brainSize : brains()) {
            size += brainSize;
        }
        return size;
    }

    protected static void waitUntilCPDiscoveryCompleted(final HazelcastInstance[] instances) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    RaftService service = getRaftService(instance);
                    assertTrue(service.getMetadataGroupManager().isDiscoveryCompleted());
                }
            }
        });
    }

    protected static RaftService getRaftService(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(RaftService.SERVICE_NAME);
    }
}
