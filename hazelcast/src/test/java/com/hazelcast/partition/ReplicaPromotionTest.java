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

package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationInterceptor;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ReplicaPromotionTest extends HazelcastTestSupport {

    static final int BLOCKING_OP_COUNT = 100;
    static final CountDownLatch LATCH = new CountDownLatch(1);
    static final CountDownLatch BLOCKING_DONE_LATCH = new CountDownLatch(BLOCKING_OP_COUNT);

    @Test
    public void testPromotion_whenPartitionThreadsBlocked() throws InterruptedException {
        // given a 3-node cluster with initialized partitions
        Config config = smallInstanceConfig().setProperty(PARTITION_COUNT.getName(), "" + BLOCKING_OP_COUNT);
        // PromotionCommitOperation is initialized with max-no-heartbeat-seconds as call timeout
        config.setProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "10");
        config.getJetConfig().setEnabled(false);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        warmUpPartitions(instances);
        // when a member is terminated while partition threads are blocked for more time
        // than PromotionCommitOperation's call timeout
        OperationService operationService = Accessors.getOperationService(instances[0]);
        for (int i = 0; i < 100; i++) {
            Operation op = new BlockingOperation();
            op.setPartitionId(i);
            operationService.invokeOnPartition(op);
        }
        instances[1].getLifecycleService().terminate();
        sleepSeconds(30);
        // then promotions proceed, once partition threads are unblocked
        LATCH.countDown();
        BLOCKING_DONE_LATCH.await(10, TimeUnit.SECONDS);
        assertTrueEventually(() -> assertTrue(instances[0].getPartitionService().isClusterSafe()));
    }

    @Test
    public void testBeforePromotionFailure() {
        // fail twice, then allow before promotion to proceed
        MigrationInterceptor interceptor = new MigrationInterceptor() {
            final AtomicInteger counter = new AtomicInteger();
            @Override
            public void onPromotionStart(MigrationParticipant participant, Collection<MigrationInfo> migrations) {
                if (counter.incrementAndGet() < 3) {
                    throw new RuntimeException("Injected failure");
                }
            }
        };
        testWithInjectedFailure(interceptor);
    }

    @Test
    public void testFinalizePromotionFailure() {
        // fail twice, then allow finalize promotion to proceed
        MigrationInterceptor interceptor = new MigrationInterceptor() {
            final AtomicInteger counter = new AtomicInteger();
            @Override
            public void onPromotionFinalize(MigrationParticipant participant, Collection<MigrationInfo> migrations) {
                if (counter.incrementAndGet() < 3) {
                    throw new RuntimeException("Injected failure");
                }
            }
        };
        testWithInjectedFailure(interceptor);
    }

    @Test
    public void testCompletePromotionFailure() {
        // fail twice, then allow complete promotion to proceed
        MigrationInterceptor interceptor = new MigrationInterceptor() {
            final AtomicInteger counter = new AtomicInteger();
            @Override
            public void onPromotionComplete(MigrationParticipant participant,
                                            Collection<MigrationInfo> migrations, boolean success) {
                if (counter.incrementAndGet() < 3) {
                    throw new RuntimeException("Injected failure");
                }
            }
        };
        testWithInjectedFailure(interceptor);
    }

    private void testWithInjectedFailure(MigrationInterceptor interceptor) {
        // given a 3-node cluster with initialized partitions and an instance setup to temporarily fail beforePromotion
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(smallInstanceConfig(), 3);
        warmUpPartitions(instances);
        ((InternalPartitionServiceImpl) Accessors.getPartitionService(instances[2])).setMigrationInterceptor(interceptor);
        // when a member is terminated
        instances[1].getLifecycleService().terminate();
        // then promotions are retried and eventually succeed
        assertTrueEventually(() -> assertTrue(instances[0].getPartitionService().isClusterSafe()));
    }

    public static class BlockingOperation extends Operation {

        public BlockingOperation() {
        }

        @Override
        public void run() throws Exception {
            LATCH.await(2, TimeUnit.MINUTES);
            BLOCKING_DONE_LATCH.countDown();
        }
    }

}
