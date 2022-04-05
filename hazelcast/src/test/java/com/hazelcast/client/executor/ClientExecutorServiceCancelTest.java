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

package com.hazelcast.client.executor;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.test.executor.tasks.CancellationAwareTask;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.executor.ExecutorServiceTestSupport;
import com.hazelcast.executor.LocalExecutorStats;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationInterceptor;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
/*
 * This test is failing because of order problem between actual invoke and cancel.
 * For random and partition, the reason of broken order is also unknown to me (@sancar)
 * For submit to member, it is because we do not have order guarantee in the first place.
 * and when there is partition movement, we can not is partition ID since tasks will not move with partitions
 */ public class ClientExecutorServiceCancelTest
        extends HazelcastTestSupport {

    private static final int SLEEP_TIME = 1000000;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance server1;
    private HazelcastInstance server2;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        Config config = new XmlConfigBuilder(getClass().getClassLoader().getResourceAsStream("hazelcast-test-executor.xml"))
                .build();
        server1 = hazelcastFactory.newHazelcastInstance(config);
        server2 = hazelcastFactory.newHazelcastInstance(config);
    }

    private HazelcastInstance createClient(boolean smartRouting)
            throws IOException {
        ClientConfig config = new XmlClientConfigBuilder("classpath:hazelcast-client-test-executor.xml").build();
        config.getNetworkConfig().setSmartRouting(smartRouting);
        return hazelcastFactory.newHazelcastClient(config);
    }

    @Test(expected = CancellationException.class)
    @Ignore
    public void testCancel_submitRandom_withSmartRouting()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitRandom(true);
    }

    @Test(expected = CancellationException.class)
    @Ignore
    public void testCancel_submitRandom_withDummyRouting()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitRandom(false);
    }

    private void testCancel_submitRandom(boolean smartRouting)
            throws ExecutionException, InterruptedException, IOException {
        HazelcastInstance client = createClient(smartRouting);

        IExecutorService executorService = client.getExecutorService(randomString());
        Future<Boolean> future = executorService.submit(new CancellationAwareTask(SLEEP_TIME));
        boolean cancelled = future.cancel(true);
        assertTrue(cancelled);
        future.get();
    }

    @Test(expected = CancellationException.class)
    @Ignore
    public void testCancel_submitToMember1_withSmartRouting()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitToMember(true, server1, false);
    }

    @Test(expected = CancellationException.class)
    @Ignore
    public void testCancel_submitToMember2_withSmartRouting()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitToMember(true, server2, false);
    }

    @Test(expected = CancellationException.class)
    public void testCancel_submitToMember1_withSmartRouting_WaitTaskStart()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitToMember(true, server1, true);
    }

    @Test(expected = CancellationException.class)
    public void testCancel_submitToMember2_withSmartRouting_WaitTaskStart()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitToMember(true, server2, true);
    }

    @Test(expected = CancellationException.class)
    @Ignore
    public void testCancel_submitToMember1_withDummyRouting()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitToMember(false, server1, false);
    }

    @Test(expected = CancellationException.class)
    @Ignore
    public void testCancel_submitToMember2_withDummyRouting()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitToMember(false, server2, false);
    }

    @Test(expected = CancellationException.class)
    public void testCancel_submitToMember1_withDummyRouting_WaitTaskStart()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitToMember(false, server1, true);
    }

    @Test(expected = CancellationException.class)
    public void testCancel_submitToMember2_withDummyRouting_WaitTaskStart()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitToMember(false, server2, true);
    }

    private void testCancel_submitToMember(boolean smartRouting, HazelcastInstance server, boolean waitTaskStart)
            throws ExecutionException, InterruptedException, IOException {

        HazelcastInstance client = createClient(smartRouting);

        IExecutorService executorService = client.getExecutorService(randomString());
        Future<Boolean> future = executorService
                .submitToMember(new CancellationAwareTask(SLEEP_TIME), server.getCluster().getLocalMember());

        if (waitTaskStart) {
            awaitTaskStartAtMember(server, 1);
        }

        boolean cancelled = future.cancel(true);
        assertTrue(cancelled);

        awaitTaskCancelAtMember(server, 1);

        future.get();
    }

    @Test(expected = CancellationException.class)
    @Ignore
    public void testCancel_submitToKeyOwner_withSmartRouting()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitToKeyOwner(true, false);
    }

    @Test(expected = CancellationException.class)
    @Ignore
    public void testCancel_submitToKeyOwner_withDummyRouting()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitToKeyOwner(false, false);
    }

    @Test(expected = CancellationException.class)
    public void testCancel_submitToKeyOwner_withSmartRouting_WaitTaskStart()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitToKeyOwner(true, true);
    }

    @Test(expected = CancellationException.class)
    public void testCancel_submitToKeyOwner_withDummyRouting_WaitTaskStart()
            throws ExecutionException, InterruptedException, IOException {
        testCancel_submitToKeyOwner(false, true);
    }

    private void testCancel_submitToKeyOwner(boolean smartRouting, boolean waitTaskStart)
            throws ExecutionException, InterruptedException, IOException {
        HazelcastInstance client = createClient(smartRouting);

        IExecutorService executorService = client.getExecutorService(randomString());
        String key = generateKeyOwnedBy(server1);

        Future<Boolean> future = executorService.submitToKeyOwner(new CancellationAwareTask(SLEEP_TIME), key);

        if (waitTaskStart) {
            awaitTaskStartAtMember(server1, 1);
        }

        boolean cancelled = future.cancel(true);
        assertTrue(cancelled);

        awaitTaskCancelAtMember(server1, 1);

        future.get();
    }

    @Test(expected = CancellationException.class)
    public void testCancel_submitToKeyOwner_Should_Be_Retried_While_Migrating()
            throws IOException, ExecutionException, InterruptedException {
        HazelcastInstance client = createClient(true);

        IExecutorService executorService = client.getExecutorService(randomString());
        String key = ExecutorServiceTestSupport.generateKeyOwnedBy(server1);

        Future<Boolean> future = executorService.submitToKeyOwner(new CancellationAwareTask(SLEEP_TIME), key);
        awaitTaskStartAtMember(server1, 1);

        final InternalPartitionServiceImpl internalPartitionService = (InternalPartitionServiceImpl) TestUtil.getNode(server1)
                .getPartitionService();
        final int partitionId = internalPartitionService.getPartitionId(key);
        // Simulate partition thread blockage as if the partition is migrating
        internalPartitionService.getPartitionStateManager().trySetMigratingFlag(partitionId);

        spawn(() -> {
            sleepSeconds(2);

            // Simulate migration completion
            internalPartitionService.getPartitionStateManager().clearMigratingFlag(partitionId);

        });

        // The cancel operation should not be blocked due to the blocked partition thread
        future.cancel(true);

        future.get();
    }

    @Test(expected = CancellationException.class)
    public void testCancel_submitToKeyOwner_Should_Not_Block_Migration()
            throws IOException, ExecutionException, InterruptedException {
        server2.shutdown();

        HazelcastInstance client = createClient(true);

        warmUpPartitions(server1);

        IExecutorService executorService = client.getExecutorService(randomString());
        String key = ExecutorServiceTestSupport.generateKeyOwnedBy(server1);

        final Future<Boolean> future = executorService.submitToKeyOwner(new CancellationAwareTask(SLEEP_TIME), key);
        awaitTaskStartAtMember(server1, 1);

        InternalPartitionServiceImpl internalPartitionService = (InternalPartitionServiceImpl) TestUtil.getNode(server1)
                .getPartitionService();
        final int partitionId = internalPartitionService.getPartitionId(key);
        // Simulate partition thread blockage as if the partition is migrating
        internalPartitionService.setMigrationInterceptor(new MigrationInterceptor() {
            @Override
            public void onMigrationCommit(MigrationInterceptor.MigrationParticipant participant, MigrationInfo migrationInfo) {
                int migratingPartitionId = migrationInfo.getPartitionId();
                if (migratingPartitionId == partitionId) {
                    spawn(() -> {
                        future.cancel(true);
                    });

                    //sleep enough so that the ExecutorServiceCancelOnPartitionMessageTask actually starts
                    // This test is time sensitive
                    sleepSeconds(3);
                }
            }
        });

        // Start the second member to initiate migration
        server2 = hazelcastFactory.newHazelcastInstance();

        waitAllForSafeState(server1, server2);

        future.get();
    }

    private void awaitTaskStartAtMember(final HazelcastInstance member, final long startedTaskCount) {
        assertTrueEventually(() -> {
            LocalExecutorStats executorStats = getMemberLocalExecutorStats(member);
            assertEquals(startedTaskCount, executorStats.getStartedTaskCount());
        });
    }

    private void awaitTaskCancelAtMember(final HazelcastInstance member, final long cancelledTaskCount) {
        assertTrueEventually(() -> {
            LocalExecutorStats executorStats = getMemberLocalExecutorStats(member);
            assertEquals(cancelledTaskCount, executorStats.getCancelledTaskCount());
        });
    }

    protected LocalExecutorStatsImpl getMemberLocalExecutorStats(HazelcastInstance member) {
        final ServiceManager serviceManager = TestUtil.getNode(member).getNodeEngine().getServiceManager();
        final DistributedExecutorService distributedExecutorService = serviceManager
                .getService(DistributedExecutorService.SERVICE_NAME);

        final Map<String, LocalExecutorStatsImpl> allStats = distributedExecutorService.getStats();
        Iterator<LocalExecutorStatsImpl> statsIterator = allStats.values().iterator();
        assertTrue(statsIterator.hasNext());
        return statsIterator.next();
    }
}
