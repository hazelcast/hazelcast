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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.Partition;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.RingbufferStore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.google.common.collect.Lists.newArrayList;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.RingbufferConfig.DEFAULT_CAPACITY;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferStoreFailureConsistencyTest extends HazelcastTestSupport {

    private static final String RINGBUFFER_NAME = "testRingbuffer";
    private static final String INIT_VALUE = "INIT";
    private static final String ONE = "One";
    private static final String TWO = "Two";
    private static final String THREE = "Three";

    @Mock
    private RingbufferStore<String> store;
    private Ringbuffer<String> ringbufferPrimary;
    private Ringbuffer<String> ringbufferBackup;
    private HazelcastInstance primaryInstance;

    private static Config getConfig(String ringbufferName, int capacity, RingbufferStoreConfig ringbufferStoreConfig) {
        Config config = new Config();
        RingbufferConfig rbConfig = config
                .getRingbufferConfig(ringbufferName)
                .setInMemoryFormat(OBJECT)
                .setBackupCount(1)
                .setCapacity(capacity);
        rbConfig.setRingbufferStoreConfig(ringbufferStoreConfig);
        return config;
    }

    @Before
    public void setUp() {
        initMocks(this);

        RingbufferStoreConfig rbStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setStoreImplementation(store);
        Config config = getConfig(RINGBUFFER_NAME, DEFAULT_CAPACITY, rbStoreConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        primaryInstance = getPrimaryInstance(instance, instance2);
        HazelcastInstance backupInstance = getBackupInstance(instance, instance2);

        ringbufferPrimary = primaryInstance.getRingbuffer(RINGBUFFER_NAME);
        ringbufferBackup = backupInstance.getRingbuffer(RINGBUFFER_NAME);
    }

    @Test
    public void testAdd_PrimaryAndBackupIsConsistentAfterStoreFailure() throws Exception {
        long seqInit = ringbufferPrimary.add(INIT_VALUE);
        long seqTwo = seqInit;
        doThrow(new IllegalStateException("Expected test exception")).when(store).store(seqInit + 2, TWO);

        long seqOne = ringbufferPrimary.add(ONE);
        try {
            seqTwo = ringbufferPrimary.add(TWO);
        } catch (HazelcastException expected) {
            // do nothing
        }
        long seqThree = ringbufferPrimary.add(THREE);

        verifySecondItemWasNotAdded(ringbufferPrimary, seqOne, seqTwo, seqThree);
        terminatePrimary();
        verifySecondItemWasNotAdded(ringbufferBackup, seqOne, seqTwo, seqThree);
    }

    @Test
    public void testAddAsync_PrimaryAndBackupIsConsistentAfterStoreFailure() throws Exception {
        long seqInit = ringbufferPrimary.add(INIT_VALUE);
        long seqTwo = seqInit;
        doThrow(new IllegalStateException("Expected test exception")).when(store).store(seqInit + 2, TWO);

        Future<Long> seqOneFuture = ringbufferPrimary.addAsync(ONE, OverflowPolicy.OVERWRITE).toCompletableFuture();
        Future<Long> seqTwoFuture = ringbufferPrimary.addAsync(TWO, OverflowPolicy.OVERWRITE).toCompletableFuture();
        Future<Long> seqThreeFuture = ringbufferPrimary.addAsync(THREE, OverflowPolicy.OVERWRITE).toCompletableFuture();

        long seqOne = seqOneFuture.get();
        try {
            seqTwo = seqTwoFuture.get();
        } catch (ExecutionException expected) {
            // do nothing
        }
        long seqThree = seqThreeFuture.get();

        verifySecondItemWasNotAdded(ringbufferPrimary, seqOne, seqTwo, seqThree);
        terminatePrimary();
        verifySecondItemWasNotAdded(ringbufferBackup, seqOne, seqTwo, seqThree);
    }

    @Test
    public void testAddAllAsync_PrimaryAndBackupIsConsistentAfterStoreFailure() throws Exception {
        long primaryTailSequenceBeforeAddingAll = ringbufferPrimary.tailSequence();
        long seqFirstItem = ringbufferPrimary.tailSequence() + 1;
        doThrow(new IllegalStateException("Expected test exception")).when(store).storeAll(eq(seqFirstItem),
                (String[]) any(Object[].class));

        Future<Long> result = ringbufferPrimary.addAllAsync(newArrayList(ONE, TWO, THREE), OverflowPolicy.FAIL)
                                               .toCompletableFuture();
        try {
            result.get();
        } catch (ExecutionException expected) {
            // do nothing
        }
        long primarySequenceAfterAddingAll = ringbufferPrimary.tailSequence();

        assertEquals(primaryTailSequenceBeforeAddingAll, primarySequenceAfterAddingAll);
        terminatePrimary();
        assertEquals(primarySequenceAfterAddingAll, ringbufferBackup.tailSequence());
    }

    private HazelcastInstance getPrimaryInstance(HazelcastInstance instance, HazelcastInstance instance2) {
        Partition primaryPartition = instance.getPartitionService().getPartition(RINGBUFFER_NAME);
        UUID primaryInstanceUuid = primaryPartition.getOwner().getUuid();
        UUID instanceOneUuid = instance.getCluster().getLocalMember().getUuid();
        return primaryInstanceUuid.equals(instanceOneUuid) ? instance : instance2;
    }

    private HazelcastInstance getBackupInstance(HazelcastInstance instance, HazelcastInstance instance2) {
        Partition primaryPartition = instance.getPartitionService().getPartition(RINGBUFFER_NAME);
        UUID primaryInstanceUuid = primaryPartition.getOwner().getUuid();
        UUID instanceOneUuid = instance.getCluster().getLocalMember().getUuid();
        return primaryInstanceUuid.equals(instanceOneUuid) ? instance2 : instance;
    }

    private void verifySecondItemWasNotAdded(Ringbuffer<String> ringbuffer, long seqOne, long seqTwo, long seqThree)
            throws InterruptedException {
        assertEquals(ONE, ringbuffer.readOne(seqOne));
        assertEquals(INIT_VALUE, ringbuffer.readOne(seqTwo));
        assertEquals(ONE, ringbuffer.readOne(seqThree - 1));
        assertEquals(THREE, ringbuffer.readOne(seqThree));
    }

    private void terminatePrimary() {
        primaryInstance.getLifecycleService().terminate();
    }
}
