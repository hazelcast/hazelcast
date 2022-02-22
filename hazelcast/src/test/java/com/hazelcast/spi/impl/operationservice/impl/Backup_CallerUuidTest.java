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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;

/**
 * A unit test that verifies that there is no need to explicitly pass the callerUuid.
 * <p>
 * See the following PR for more detail:
 * https://github.com/hazelcast/hazelcast/pull/8173
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Backup_CallerUuidTest extends HazelcastTestSupport {

    private static final AtomicReference CALLER_UUID = new AtomicReference();

    private HazelcastInstance hz;

    @Before
    public void setup() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(cluster);
        hz = cluster[0];
    }

    @Test
    public void test() {
        InternalCompletableFuture f = getOperationService(hz).invokeOnPartition(new DummyUpdateOperation()
                .setPartitionId(getPartitionId(hz)));
        assertCompletesEventually(f);

        assertEquals(CALLER_UUID.get(), hz.getCluster().getLocalMember().getUuid());
    }

    private static class DummyUpdateOperation extends Operation implements BackupAwareOperation {
        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return 1;
        }

        @Override
        public int getAsyncBackupCount() {
            return 0;
        }

        @Override
        public Operation getBackupOperation() {
            return new DummyBackupOperation();
        }

        @Override
        public void run() throws Exception {
        }
    }

    private static class DummyBackupOperation extends Operation implements BackupOperation {
        @Override
        public void run() throws Exception {
            CALLER_UUID.set(getCallerUuid());
        }
    }
}
