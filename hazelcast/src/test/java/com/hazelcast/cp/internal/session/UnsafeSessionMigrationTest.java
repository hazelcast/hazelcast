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

package com.hazelcast.cp.internal.session;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeSessionMigrationTest extends HazelcastRaftTestSupport {

    @Test
    public void whenPartitionIsMigrated_thenSessionInformationShouldMigrate() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2");

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        RaftGroupId group1 = getRaftService(hz1).createRaftGroupForProxy(generateName(hz1, 0));
        RaftGroupId group2 = getRaftService(hz1).createRaftGroupForProxy(generateName(hz1, 1));

        ProxySessionManagerService sessionManager = getSessionManager(hz1);

        long session1 = sessionManager.acquireSession(group1);
        long session2 = sessionManager.acquireSession(group2);

        // Start new instance to trigger migration
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        waitAllForSafeState(hz1, hz2);

        // Unlock after lock is migrated
        sessionManager.heartbeat(group1, session1).joinInternal();
        sessionManager.heartbeat(group2, session2).joinInternal();
    }

    private ProxySessionManagerService getSessionManager(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(ProxySessionManagerService.SERVICE_NAME);
    }

    private String generateName(HazelcastInstance instance, int partitionId) {
        return "group@" + generateKeyForPartition(instance, partitionId);
    }
}
