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

package com.hazelcast.splitbrainprotection;

import com.hazelcast.cache.ICache;
import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.CountDownLatchConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.lock.ILock;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.io.Serializable;

import static com.hazelcast.splitbrainprotection.PartitionedCluster.SPLIT_BRAIN_PROTECTION_ID;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.WRITE;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ_WRITE;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static java.util.Arrays.asList;

/**
 * Base class for all split brain protection tests.
 * <p>
 * It defines split brain protection and data-structures that use it.
 * Then it initialises and splits the cluster into two parts:
 * <ul>
 * <li>3 nodes -> this sub-cluster matches the split brain protection requirements</li>
 * <li>2 nodes -> this sub-cluster DOES NOT match the split brain protection requirements</li>
 * </ul>
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractSplitBrainProtectionTest {

    protected static final String SEMAPHORE = "splitBrainProtection" + randomString();
    protected static final String REFERENCE_NAME = "reference" + "splitBrainProtection" + randomString();
    protected static final String LONG_NAME = "long" + "splitBrainProtection" + randomString();
    protected static final String CACHE_NAME = "splitBrainProtection" + randomString();
    protected static final String ESTIMATOR_NAME = "splitBrainProtection" + randomString();
    protected static final String LATCH_NAME = "splitBrainProtection" + randomString();
    protected static final String DURABLE_EXEC_NAME = "splitBrainProtection" + randomString();
    protected static final String EXEC_NAME = "splitBrainProtection" + randomString();
    protected static final String LIST_NAME = "splitBrainProtection" + randomString();
    protected static final String LOCK_NAME = "splitBrainProtection" + randomString();
    protected static final String MAP_NAME = "splitBrainProtection" + randomString();
    protected static final String MULTI_MAP_NAME = "splitBrainProtection" + randomString();
    protected static final String QUEUE_NAME = "splitBrainProtection" + randomString();
    protected static final String REPLICATED_MAP_NAME = "splitBrainProtection" + randomString();
    protected static final String RINGBUFFER_NAME = "splitBrainProtection" + randomString();
    protected static final String SCHEDULED_EXEC_NAME = "splitBrainProtection" + randomString();
    protected static final String SET_NAME = "splitBrainProtection" + randomString();
    protected static final String PN_COUNTER_NAME = "splitBrainProtection" + randomString();

    protected static PartitionedCluster cluster;
    protected static TestHazelcastInstanceFactory factory;

    protected static void initTestEnvironment(Config config,
                                              TestHazelcastInstanceFactory factory) {
        if (AbstractSplitBrainProtectionTest.factory != null) {
            throw new IllegalStateException("Already initialised!");
        }
        AbstractSplitBrainProtectionTest.factory = factory;
        initCluster(PartitionedCluster.createClusterConfig(config), factory, READ, WRITE, READ_WRITE);
    }

    protected static void shutdownTestEnvironment() {
        factory.terminateAll();
        factory = null;
        cluster = null;
    }

    protected static SemaphoreConfig newSemaphoreConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig();
        semaphoreConfig.setName(SEMAPHORE + splitBrainProtectionOn.name());
        semaphoreConfig.setSplitBrainProtectionName(splitBrainProtectionName);
        return semaphoreConfig;
    }

    protected static AtomicReferenceConfig newAtomicReferenceConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        AtomicReferenceConfig config = new AtomicReferenceConfig(REFERENCE_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static AtomicLongConfig newAtomicLongConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        AtomicLongConfig config = new AtomicLongConfig(LONG_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static CacheSimpleConfig newCacheConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        CacheSimpleConfig config = new CacheSimpleConfig();
        config.setName(CACHE_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static CardinalityEstimatorConfig newEstimatorConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        CardinalityEstimatorConfig config = new CardinalityEstimatorConfig(ESTIMATOR_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static CountDownLatchConfig newLatchConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        CountDownLatchConfig config = new CountDownLatchConfig(LATCH_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static DurableExecutorConfig newDurableExecConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                                String splitBrainProtectionName, String postfix) {
        DurableExecutorConfig config = new DurableExecutorConfig(DURABLE_EXEC_NAME + splitBrainProtectionOn.name() + postfix);
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static ExecutorConfig newExecConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                  String splitBrainProtectionName, String postfix) {
        ExecutorConfig config = new ExecutorConfig(EXEC_NAME + splitBrainProtectionOn.name() + postfix);
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static ListConfig newListConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        ListConfig config = new ListConfig(LIST_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static LockConfig newLockConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        LockConfig config = new LockConfig(LOCK_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static MapConfig newMapConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        MapConfig config = new MapConfig(MAP_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static MultiMapConfig newMultiMapConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        MultiMapConfig config = new MultiMapConfig(MULTI_MAP_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static QueueConfig newQueueConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        QueueConfig config = new QueueConfig(QUEUE_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        config.setBackupCount(4);
        return config;
    }

    protected static ReplicatedMapConfig newReplicatedMapConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                                String splitBrainProtectionName) {
        ReplicatedMapConfig config = new ReplicatedMapConfig(REPLICATED_MAP_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static RingbufferConfig newRingbufferConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                          String splitBrainProtectionName) {
        RingbufferConfig config = new RingbufferConfig(RINGBUFFER_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        config.setBackupCount(4);
        return config;
    }

    protected static ScheduledExecutorConfig newScheduledExecConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                                    String splitBrainProtectionName, String postfix) {
        ScheduledExecutorConfig config = new ScheduledExecutorConfig(SCHEDULED_EXEC_NAME + splitBrainProtectionOn.name() + postfix);
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static SetConfig newSetConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        SetConfig config = new SetConfig(SET_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static PNCounterConfig newPNCounterConfig(SplitBrainProtectionOn splitBrainProtectionOn, String splitBrainProtectionName) {
        PNCounterConfig config = new PNCounterConfig(PN_COUNTER_NAME + splitBrainProtectionOn.name());
        config.setSplitBrainProtectionName(splitBrainProtectionName);
        return config;
    }

    protected static SplitBrainProtectionConfig newSplitBrainProtectionConfig(SplitBrainProtectionOn splitBrainProtectionOn,
                                                                              String splitBrainProtectionName) {
        SplitBrainProtectionConfig splitBrainProtectionConfig = new SplitBrainProtectionConfig();
        splitBrainProtectionConfig.setName(splitBrainProtectionName);
        splitBrainProtectionConfig.setProtectOn(splitBrainProtectionOn);
        splitBrainProtectionConfig.setEnabled(true);
        splitBrainProtectionConfig.setMinimumClusterSize(3);
        return splitBrainProtectionConfig;
    }

    protected static void initCluster(Config config, TestHazelcastInstanceFactory factory, SplitBrainProtectionOn... types) {
        cluster = new PartitionedCluster(factory);

        String[] splitBrainProtectionNames = new String[types.length];
        int i = 0;
        for (SplitBrainProtectionOn splitBrainProtectionOn : types) {
            String splitBrainProtectionName = SPLIT_BRAIN_PROTECTION_ID + splitBrainProtectionOn.name();
            config.addSplitBrainProtectionConfig(newSplitBrainProtectionConfig(splitBrainProtectionOn, splitBrainProtectionName));
            splitBrainProtectionNames[i++] = splitBrainProtectionName;

            config.addSemaphoreConfig(newSemaphoreConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addAtomicReferenceConfig(newAtomicReferenceConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addAtomicLongConfig(newAtomicLongConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addCacheConfig(newCacheConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addCardinalityEstimatorConfig(newEstimatorConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addCountDownLatchConfig(newLatchConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addListConfig(newListConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addLockConfig(newLockConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addMapConfig(newMapConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addMultiMapConfig(newMultiMapConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addQueueConfig(newQueueConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addReplicatedMapConfig(newReplicatedMapConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addRingBufferConfig(newRingbufferConfig(splitBrainProtectionOn, splitBrainProtectionName));
            for (String postfix : asList("", "shutdown", "shutdownNow")) {
                config.addDurableExecutorConfig(newDurableExecConfig(splitBrainProtectionOn, splitBrainProtectionName, postfix));
                config.addExecutorConfig(newExecConfig(splitBrainProtectionOn, splitBrainProtectionName, postfix));
                config.addScheduledExecutorConfig(newScheduledExecConfig(splitBrainProtectionOn, splitBrainProtectionName, postfix));
            }
            config.addSetConfig(newSetConfig(splitBrainProtectionOn, splitBrainProtectionName));
            config.addPNCounterConfig(newPNCounterConfig(splitBrainProtectionOn, splitBrainProtectionName));
        }

        cluster.createFiveMemberCluster(config);
        initData(types);
        cluster.splitFiveMembersThreeAndTwo(splitBrainProtectionNames);
    }

    private static void initData(SplitBrainProtectionOn[] types) {
        for (SplitBrainProtectionOn splitBrainProtectionOn : types) {
            for (int element = 0; element < 10000; element++) {
                cluster.instance[0].getQueue(QUEUE_NAME + splitBrainProtectionOn.name()).offer(element);
            }
            for (int id = 0; id < 10000; id++) {
                cluster.instance[0].getRingbuffer(RINGBUFFER_NAME + splitBrainProtectionOn.name()).add(String.valueOf(id));
            }
            cluster.instance[0].getSemaphore(SEMAPHORE + splitBrainProtectionOn.name()).init(100);
        }
    }

    protected ISemaphore semaphore(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getSemaphore(SEMAPHORE + splitBrainProtectionOn.name());
    }

    protected IAtomicReference<SplitBrainProtectionTestClass> aref(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getAtomicReference(REFERENCE_NAME + splitBrainProtectionOn.name());
    }

    protected IAtomicLong along(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getAtomicLong(LONG_NAME + splitBrainProtectionOn.name());
    }

    protected ICache<Integer, String> cache(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getCacheManager().getCache(CACHE_NAME + splitBrainProtectionOn.name());
    }

    protected CardinalityEstimator estimator(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getCardinalityEstimator(ESTIMATOR_NAME + splitBrainProtectionOn.name());
    }

    protected ICountDownLatch latch(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getCountDownLatch(LATCH_NAME + splitBrainProtectionOn.name());
    }

    protected DurableExecutorService durableExec(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return durableExec(index, splitBrainProtectionOn, "");
    }

    protected DurableExecutorService durableExec(int index, SplitBrainProtectionOn splitBrainProtectionOn, String postfix) {
        return cluster.instance[index].getDurableExecutorService(DURABLE_EXEC_NAME + splitBrainProtectionOn.name() + postfix);
    }

    protected IExecutorService exec(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return exec(index, splitBrainProtectionOn, "");
    }

    protected IExecutorService exec(int index, SplitBrainProtectionOn splitBrainProtectionOn, String postfix) {
        return cluster.instance[index].getExecutorService(EXEC_NAME + splitBrainProtectionOn.name() + postfix);
    }

    protected IList list(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getList(LIST_NAME + splitBrainProtectionOn.name());
    }

    protected ILock lock(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getLock(LOCK_NAME + splitBrainProtectionOn.name());
    }

    protected IMap map(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getMap(MAP_NAME + splitBrainProtectionOn.name());
    }

    protected MultiMap multimap(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getMultiMap(MULTI_MAP_NAME + splitBrainProtectionOn.name());
    }

    protected IQueue queue(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getQueue(QUEUE_NAME + splitBrainProtectionOn.name());
    }

    protected ReplicatedMap replmap(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getReplicatedMap(REPLICATED_MAP_NAME + splitBrainProtectionOn.name());
    }

    protected Ringbuffer ring(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getRingbuffer(RINGBUFFER_NAME + splitBrainProtectionOn.name());
    }

    protected IScheduledExecutorService scheduledExec(int index, SplitBrainProtectionOn splitBrainProtectionOn, String postfix) {
        return cluster.instance[index].getScheduledExecutorService(SCHEDULED_EXEC_NAME + splitBrainProtectionOn.name() + postfix);
    }

    protected ISet set(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getSet(SET_NAME + splitBrainProtectionOn.name());
    }

    protected PNCounter pnCounter(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return cluster.instance[index].getPNCounter(PN_COUNTER_NAME + splitBrainProtectionOn.name());
    }

    protected static IFunction function() {
        return new IFunction<Object, Object>() {
            @Override
            public Object apply(Object input) {
                return input;
            }
        };
    }

    public static class SplitBrainProtectionTestClass implements Serializable {

        public static SplitBrainProtectionTestClass object() {
            return new SplitBrainProtectionTestClass();
        }
    }
}
