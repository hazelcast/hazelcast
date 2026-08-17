/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.migration;

import com.hazelcast.config.Config;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.spi.properties.ClusterProperty;

/**
 * Test of backup operations in case of maximum allowed number of members crash
 */
public class VectorCollectionMaxMemberCrashBackupTest extends VectorCollectionBackupTestBase {

    @Override
    protected int getNodeCount() {
        return VectorCollectionConfig.MAX_BACKUP_COUNT + 1;
    }

    @Override
    protected int getBackupCount() {
        return getNodeCount() - 1;
    }

    @Override
    protected Config getConfig() {
        return super.getConfig()
                // disable anti entropy and rely only on backup operations
                .setProperty(ClusterProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName(), "1000");
    }

    @Override
    protected <S, T> void runScenario(FunctionEx<HazelcastInstance, S> init,
                                      BiFunctionEx<HazelcastInstance, S, T> action,
                                      ConsumerEx<T> validation) {
        // 1. run action
        // 2. terminate all members except one
        // 3. wait for partition-related changes
        // 4. validate no data lost

        var toBeTerminated = members[1];
        var state1 = init.apply(toBeTerminated);
        var state2 = action.apply(toBeTerminated, state1);
        // Some operations always have async backups (clear, optimize, member-side putAll).
        // In this test scenario we are not interested in lost/slow backups - this is tested
        // by VectorCollectionBackupSlowBackupTest.
        // Ensure that async backups, if any, were executed before crash to avoid flakiness.
        waitAllForSafeState(members);

        for (int i = 1; i < members.length; i++) {
            members[i].getLifecycleService().terminate();
        }
        assertClusterSizeEventually(1, members[0]);
        waitAllForSafeState(members[0]);
        validation.accept(state2);
    }
}
