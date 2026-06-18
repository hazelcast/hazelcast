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

package com.hazelcast.vector.impl.migration;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.server.PacketFilter;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.PacketFiltersUtil;
import org.junit.Ignore;
import org.junit.Test;

import static com.hazelcast.test.Accessors.getNode;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test anti-entropy in case of slow backup - can detect cases when we do not wait for acks
 */
public class VectorCollectionBackupSlowBackupTest extends VectorCollectionBackupTestBase {

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
        // 0. initialize
        // 1. delay backups
        // 2. run action
        // 3. terminate member
        // 4. wait for partition-related changes
        // 5. validate no data lost

        var toBeTerminated = members[1];
        var state1 = init.apply(toBeTerminated);

        PacketFiltersUtil.setCustomFilter(toBeTerminated, new VectorCollectionBackupLostBackupTest.BackupPacketDropFilter(
                getNode(toBeTerminated).getSerializationService(), 1.0f, PacketFilter.Action.DELAY));

        var state2 = action.apply(toBeTerminated, state1);

        // the backups are only delayed, not lost, so we do not wait for anti-entropy
        // sync backups should report their results before the scenario proceeds

        toBeTerminated.getLifecycleService().terminate();
        waitAllForSafeState(members[0], members[2]);
        validation.accept(state2);
    }

    @Override
    public void testPutAllOne() {
        assumeThat(useClient).as("Member API invocation does not wait for backup acks").isTrue();
        super.testPutAllOne();
    }

    @Override
    public void testPutAllSinglePartition() {
        assumeThat(useClient).as("Member API invocation does not wait for backup acks").isTrue();
        super.testPutAllSinglePartition();
    }

    @Override
    public void testPutAll() {
        assumeThat(useClient).as("Member API invocation does not wait for backup acks").isTrue();
        super.testPutAll();
    }

    @Test
    @Override
    @Ignore("invokeOnAllPartitionsAsync does not wait for backup acks")
    public void testClear() {
        // invokeOnAllPartitionsAsync is used both for client and member API invocation
        super.testClear();
    }
}
