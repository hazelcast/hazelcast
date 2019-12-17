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

package com.hazelcast.internal.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.operation.FetchPartitionStateOperation;
import com.hazelcast.internal.partition.operation.MigrationOperation;
import com.hazelcast.internal.partition.operation.MigrationRequestOperation;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.partition.IPartitionService.SERVICE_NAME;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MasterSplitTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory();
    }

    @Test
    public void test_migrationFailsOnMasterMismatch_onSource() throws InterruptedException {
        HazelcastInstance member1 = factory.newHazelcastInstance();
        HazelcastInstance member2 = factory.newHazelcastInstance();

        warmUpPartitions(member1, member2);

        MigrationInfo migration = createMigrationInfo(member1, member2);

        int partitionStateVersion = getPartitionService(member1).getPartitionStateVersion();

        Operation op = new MigrationRequestOperation(migration, Collections.<MigrationInfo>emptyList(), partitionStateVersion, true);

        InvocationBuilder invocationBuilder = getOperationServiceImpl(member1)
                .createInvocationBuilder(SERVICE_NAME, op, getAddress(member2))
                .setCallTimeout(TimeUnit.MINUTES.toMillis(1));
        Future future = invocationBuilder.invoke();

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    private MigrationInfo createMigrationInfo(HazelcastInstance master, HazelcastInstance nonMaster) {
        MigrationInfo migration
                = new MigrationInfo(getPartitionId(nonMaster), new PartitionReplica(getAddress(nonMaster), getNode(nonMaster).getThisUuid()),
                new PartitionReplica(getAddress(master), getNode(master).getThisUuid()), 0, 1, -1, 0);
        migration.setMaster(getAddress(nonMaster));
        return migration;
    }

    @Test
    public void test_migrationFailsOnMasterMismatch_onDestination() throws InterruptedException {
        HazelcastInstance member1 = factory.newHazelcastInstance();
        HazelcastInstance member2 = factory.newHazelcastInstance();

        warmUpPartitions(member1, member2);

        MigrationInfo migration = createMigrationInfo(member1, member2);

        int partitionStateVersion = getPartitionService(member1).getPartitionStateVersion();

        ReplicaFragmentMigrationState migrationState
                = new ReplicaFragmentMigrationState(Collections.<ServiceNamespace, long[]>emptyMap(), Collections.<Operation>emptySet());
        Operation op = new MigrationOperation(migration, Collections.<MigrationInfo>emptyList(), partitionStateVersion, migrationState, true, true);

        InvocationBuilder invocationBuilder = getOperationServiceImpl(member1)
                .createInvocationBuilder(SERVICE_NAME, op, getAddress(member2))
                .setCallTimeout(TimeUnit.MINUTES.toMillis(1));
        Future future = invocationBuilder.invoke();

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void test_fetchPartitionStateFailsOnMasterMismatch()
            throws InterruptedException {
        HazelcastInstance member1 = factory.newHazelcastInstance();
        HazelcastInstance member2 = factory.newHazelcastInstance();
        HazelcastInstance member3 = factory.newHazelcastInstance();

        warmUpPartitions(member1, member2, member3);

        InternalCompletableFuture<Object> future = getOperationServiceImpl(member2)
                .createInvocationBuilder(SERVICE_NAME, new FetchPartitionStateOperation(), getAddress(member3))
                .setTryCount(Integer.MAX_VALUE).setCallTimeout(Long.MAX_VALUE).invoke();

        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

}
