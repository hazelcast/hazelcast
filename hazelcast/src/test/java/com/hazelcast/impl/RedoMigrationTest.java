/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class RedoMigrationTest extends RedoTestService {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 10000)
    public void testAddListenerInfiniteLoop() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        ConcurrentMapManager concurrentMapManager = getConcurrentMapManager(h1);
        ListenerManager lm = concurrentMapManager.node.listenerManager;
        ListenerManager.AddRemoveListener arl = lm.new AddRemoveListener("default", true, true);
        BaseManager.TargetAwareOp op = arl.createNewTargetAwareOp(new Address("127.0.0.1", 6666));
        op.doOp();
        assertEquals(Constants.Objects.OBJECT_REDO, op.getResult());
    }

    @Test(timeout = 100000)
    public void testShutdownSecondNodeWhileMigrating() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap imap1 = h1.getMap("default");
        for (int i = 0; i < 10000; i++) {
            imap1.put(i, "value" + i);
        }
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        TestUtil.migratePartition(28, h1, h2);
        assertEquals(getPartitionById(h1.getPartitionService(), 28).getOwner(), h2.getCluster().getLocalMember());
        assertEquals(getPartitionById(h2.getPartitionService(), 28).getOwner(), h2.getCluster().getLocalMember());
        assertEquals(getPartitionById(h3.getPartitionService(), 28).getOwner(), h2.getCluster().getLocalMember());
        TestUtil.initiateMigration(28, 20, h1, h2, h1);
        final ConcurrentMapManager chm2 = getConcurrentMapManager(h2);
        chm2.enqueueAndWait(new Processable() {
            public void process() {
                assertTrue(chm2.partitionManager.blocks[28].isMigrating());
            }
        }, 10);
        final CountDownLatch migrationLatch = new CountDownLatch(2);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
                System.out.println("event " + migrationEvent);
                if (migrationEvent.getPartitionId() == 28 && migrationEvent.getNewOwner().equals(h1.getCluster().getLocalMember())) {
                    migrationLatch.countDown();
                }
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
                System.out.println("ev " + migrationEvent);
            }
        };
        h3.getPartitionService().addMigrationListener(migrationListener);
        h1.getPartitionService().addMigrationListener(migrationListener);
        h2.getLifecycleService().shutdown();
        if (!migrationLatch.await(40, TimeUnit.SECONDS)) {
            for (Block block : getConcurrentMapManager(h1).partitionManager.blocks) {
                if (block.isMigrating()) {
                    System.out.println(block);
                }
            }
            for (Block block : getConcurrentMapManager(h3).partitionManager.blocks) {
                if (block.isMigrating()) {
                    System.out.println(block);
                }
            }
            fail("Migration should get completed in 20 seconds!!");
        }
    }

    @Test(timeout = 100000)
    public void testShutdownOldestMemberWhileMigrating() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap imap1 = h1.getMap("default");
        for (int i = 0; i < 10000; i++) {
            imap1.put(i, "value" + i);
        }
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        assertEquals(getPartitionById(h1.getPartitionService(), 28).getOwner(), h1.getCluster().getLocalMember());
        assertEquals(getPartitionById(h2.getPartitionService(), 28).getOwner(), h1.getCluster().getLocalMember());
        final CountDownLatch migrationLatch = new CountDownLatch(2);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
                if (migrationEvent.getPartitionId() == 28 && migrationEvent.getNewOwner().equals(h2.getCluster().getLocalMember())) {
                    migrationLatch.countDown();
                }
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
            }
        };
        h3.getPartitionService().addMigrationListener(migrationListener);
        h2.getPartitionService().addMigrationListener(migrationListener);
        TestUtil.initiateMigration(28, 20, h1, h1, h2);
        final ConcurrentMapManager chm1 = getConcurrentMapManager(h1);
        chm1.enqueueAndWait(new Processable() {
            public void process() {
                assertTrue(chm1.partitionManager.blocks[28].isMigrating());
            }
        }, 10);
        h1.getLifecycleService().shutdown();
        if (!migrationLatch.await(30, TimeUnit.SECONDS)) {
            for (Block block : getConcurrentMapManager(h2).partitionManager.blocks) {
                if (block.isMigrating() || block.getBlockId() == 28) {
                    System.out.println(block);
                }
            }
            for (Block block : getConcurrentMapManager(h3).partitionManager.blocks) {
                if (block.isMigrating() || block.getBlockId() == 28) {
                    System.out.println(block);
                }
            }
            fail("Migration should get completed in 30 seconds!!");
        }
    }

    @Test(timeout = 100000)
    public void testMapRemoteTargetPartitionInMigrationState() {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final Address address1 = ((MemberImpl) h1.getCluster().getLocalMember()).getAddress();
        final Address address2 = ((MemberImpl) h2.getCluster().getLocalMember()).getAddress();
        final int partitionId = h1.getPartitionService().getPartition(1).getPartitionId();
        final Node node1 = getNode(h1);
        final Node node2 = getNode(h2);
        final CountDownLatch migrationLatch = new CountDownLatch(1);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
                if (migrationEvent.getPartitionId() == partitionId
                        && (h2.getCluster().getLocalMember().equals(migrationEvent.getNewOwner()))) {
                    migrationLatch.countDown();
                }
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
            }
        };
        h2.getPartitionService().addMigrationListener(migrationListener);
        CallBuilder callBuilder = new KeyCallBuilder(h1);
        BeforeAfterBehavior behavior = new BeforeAfterBehavior() {
            @Override
            void before() throws Exception {
                TestUtil.migrateKey(1, h1, h2);
                node2.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        Block block = node2.concurrentMapManager.getOrCreateBlock(partitionId);
                        block.setMigrationAddress(address1);
                        assertEquals(address2, block.getOwner());
                    }
                }, 5);
            }

            @Override
            void after() {
                node1.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        node1.concurrentMapManager.partitionManager.sendBlocks(null);
                    }
                }, 5);
                try {
                    assertTrue(migrationLatch.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                }
                node2.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        Block block = node2.concurrentMapManager.getOrCreateBlock(partitionId);
                        assertEquals(address2, block.getOwner());
                    }
                }, 5);
            }
        };
        new BeforeAfterTester(behavior, callBuilder).run();
    }

    @Test(timeout = 100000)
    public void testMapLocalTargetPartitionInMigrationState() {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final Address address1 = ((MemberImpl) h1.getCluster().getLocalMember()).getAddress();
        final Address address2 = ((MemberImpl) h2.getCluster().getLocalMember()).getAddress();
        final int partitionId = h1.getPartitionService().getPartition(1).getPartitionId();
        final Node node1 = getNode(h1);
        final Node node2 = getNode(h2);
        h1.getMap("default").put(1, "value");
        h2.getMap("default").put(1, "value");
        final CountDownLatch migrationLatch = new CountDownLatch(1);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
                if (migrationEvent.getPartitionId() == partitionId
                        && (h2.getCluster().getLocalMember().equals(migrationEvent.getNewOwner()))) {
                    migrationLatch.countDown();
                }
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
            }
        };
        h2.getPartitionService().addMigrationListener(migrationListener);
        CallBuilder callBuilder = new KeyCallBuilder(h2);
        BeforeAfterBehavior behavior = new BeforeAfterBehavior() {
            @Override
            void before() throws Exception {
                TestUtil.migrateKey(1, h1, h2);
                node2.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        Block block = node2.concurrentMapManager.getOrCreateBlock(partitionId);
                        block.setMigrationAddress(address1);
                        assertEquals(address2, block.getOwner());
                    }
                }, 5);
            }

            @Override
            void after() {
                node1.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        node1.concurrentMapManager.partitionManager.sendBlocks(null);
                    }
                }, 5);
                try {
                    assertTrue(migrationLatch.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                }
                node2.clusterManager.enqueueAndWait(new Processable() {
                    public void process() {
                        Block block = node2.concurrentMapManager.getOrCreateBlock(partitionId);
                        assertEquals(address2, block.getOwner());
                    }
                }, 5);
            }
        };
        new BeforeAfterTester(behavior, callBuilder).run();
    }

    @Test(timeout = 100000)
    public void testShutdownMigrationTargetNodeWhileMigrating() throws Exception {
        Config config = new Config();
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap imap1 = h1.getMap("default");
        for (int i = 0; i < 10000; i++) {
            imap1.put(i, "value" + i);
        }
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        assertEquals(getPartitionById(h1.getPartitionService(), 28).getOwner(), h1.getCluster().getLocalMember());
        assertEquals(getPartitionById(h2.getPartitionService(), 28).getOwner(), h1.getCluster().getLocalMember());
        final CountDownLatch migrationLatch = new CountDownLatch(2);
        MigrationListener migrationListener = new MigrationListener() {
            public void migrationCompleted(MigrationEvent migrationEvent) {
                if (migrationEvent.getPartitionId() == 28 && migrationEvent.getNewOwner().equals(h1.getCluster().getLocalMember())) {
                    migrationLatch.countDown();
                }
            }

            public void migrationStarted(MigrationEvent migrationEvent) {
            }
        };
        h1.getPartitionService().addMigrationListener(migrationListener);
        h2.getPartitionService().addMigrationListener(migrationListener);
        TestUtil.initiateMigration(28, 20, h1, h1, h3);
        final ConcurrentMapManager chm1 = getConcurrentMapManager(h1);
        chm1.enqueueAndWait(new Processable() {
            public void process() {
                assertTrue(chm1.partitionManager.blocks[28].isMigrating());
            }
        }, 10);
        h3.getLifecycleService().shutdown();
        if (!migrationLatch.await(30, TimeUnit.SECONDS)) {
            for (Block block : getConcurrentMapManager(h1).partitionManager.blocks) {
                if (block.isMigrating() || block.getBlockId() == 28) {
                    System.out.println(block);
                }
            }
            for (Block block : getConcurrentMapManager(h2).partitionManager.blocks) {
                if (block.isMigrating() || block.getBlockId() == 28) {
                    System.out.println(block);
                }
            }
            fail("Migration should get completed in 30 seconds!!");
        }
    }
}
