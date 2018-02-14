/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum;

import com.hazelcast.cache.ICache;
import com.hazelcast.cardinality.CardinalityEstimator;
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
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.io.Serializable;

import static com.hazelcast.quorum.PartitionedCluster.QUORUM_ID;
import static com.hazelcast.quorum.QuorumType.READ;
import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static java.util.Arrays.asList;

/**
 * Base class for all quorum tests.
 * <p>
 * It defines quorum and data-structures that use it. Then it initialises and splits the cluster into two parts:
 * <ul>
 * <li>3 nodes -> this sub-cluster matches the quorum requirements</li>
 * <li>2 nodes -> this sub-cluster DOES NOT match the quorum requirements</li>
 * </ul>
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractQuorumTest {

    protected static final String SEMAPHORE = "quorum" + randomString();
    protected static final String REFERENCE_NAME = "reference" + "quorum" + randomString();
    protected static final String LONG_NAME = "long" + "quorum" + randomString();
    protected static final String CACHE_NAME = "quorum" + randomString();
    protected static final String ESTIMATOR_NAME = "quorum" + randomString();
    protected static final String LATCH_NAME = "quorum" + randomString();
    protected static final String DURABLE_EXEC_NAME = "quorum" + randomString();
    protected static final String EXEC_NAME = "quorum" + randomString();
    protected static final String LIST_NAME = "quorum" + randomString();
    protected static final String LOCK_NAME = "quorum" + randomString();
    protected static final String MAP_NAME = "quorum" + randomString();
    protected static final String MULTI_MAP_NAME = "quorum" + randomString();
    protected static final String QUEUE_NAME = "quorum" + randomString();
    protected static final String REPLICATED_MAP_NAME = "quorum" + randomString();
    protected static final String RINGBUFFER_NAME = "quorum" + randomString();
    protected static final String SCHEDULED_EXEC_NAME = "quorum" + randomString();
    protected static final String SET_NAME = "quorum" + randomString();
    protected static final String PN_COUNTER_NAME = "quorum" + randomString();

    protected static PartitionedCluster cluster;

    protected static void initTestEnvironment(Config config, TestHazelcastInstanceFactory factory) {
        initCluster(PartitionedCluster.createClusterConfig(config), factory, READ, WRITE, READ_WRITE);
    }

    protected static void shutdownTestEnvironment() {
        HazelcastInstanceFactory.terminateAll();
        cluster = null;
    }

    protected static SemaphoreConfig newSemaphoreConfig(QuorumType quorumType, String quorumName) {
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig();
        semaphoreConfig.setName(SEMAPHORE + quorumType.name());
        semaphoreConfig.setQuorumName(quorumName);
        return semaphoreConfig;
    }

    protected static AtomicReferenceConfig newAtomicReferenceConfig(QuorumType quorumType, String quorumName) {
        AtomicReferenceConfig config = new AtomicReferenceConfig(REFERENCE_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        return config;
    }

    protected static AtomicLongConfig newAtomicLongConfig(QuorumType quorumType, String quorumName) {
        AtomicLongConfig config = new AtomicLongConfig(LONG_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        return config;
    }

    protected static CacheSimpleConfig newCacheConfig(QuorumType quorumType, String quorumName) {
        CacheSimpleConfig config = new CacheSimpleConfig();
        config.setName(CACHE_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        return config;
    }

    protected static CardinalityEstimatorConfig newEstimatorConfig(QuorumType quorumType, String quorumName) {
        CardinalityEstimatorConfig config = new CardinalityEstimatorConfig(ESTIMATOR_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        return config;
    }

    protected static CountDownLatchConfig newLatchConfig(QuorumType quorumType, String quorumName) {
        CountDownLatchConfig config = new CountDownLatchConfig(LATCH_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        return config;
    }

    protected static DurableExecutorConfig newDurableExecConfig(QuorumType quorumType, String quorumName, String postfix) {
        DurableExecutorConfig config = new DurableExecutorConfig(DURABLE_EXEC_NAME + quorumType.name() + postfix);
        config.setQuorumName(quorumName);
        return config;
    }

    protected static ExecutorConfig newExecConfig(QuorumType quorumType, String quorumName, String postfix) {
        ExecutorConfig config = new ExecutorConfig(EXEC_NAME + quorumType.name() + postfix);
        config.setQuorumName(quorumName);
        return config;
    }

    protected static ListConfig newListConfig(QuorumType quorumType, String quorumName) {
        ListConfig config = new ListConfig(LIST_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        return config;
    }

    protected static LockConfig newLockConfig(QuorumType quorumType, String quorumName) {
        LockConfig config = new LockConfig(LOCK_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        return config;
    }

    protected static MapConfig newMapConfig(QuorumType quorumType, String quorumName) {
        MapConfig config = new MapConfig(MAP_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        return config;
    }

    protected static MultiMapConfig newMultiMapConfig(QuorumType quorumType, String quorumName) {
        MultiMapConfig config = new MultiMapConfig(MULTI_MAP_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        return config;
    }

    protected static QueueConfig newQueueConfig(QuorumType quorumType, String quorumName) {
        QueueConfig config = new QueueConfig(QUEUE_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        config.setBackupCount(4);
        return config;
    }

    protected static ReplicatedMapConfig newReplicatedMapConfig(QuorumType quorumType, String quorumName) {
        ReplicatedMapConfig config = new ReplicatedMapConfig(REPLICATED_MAP_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        return config;
    }

    protected static RingbufferConfig newRingbufferConfig(QuorumType quorumType, String quorumName) {
        RingbufferConfig config = new RingbufferConfig(RINGBUFFER_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        config.setBackupCount(4);
        return config;
    }

    protected static ScheduledExecutorConfig newScheduledExecConfig(QuorumType quorumType, String quorumName, String postfix) {
        ScheduledExecutorConfig config = new ScheduledExecutorConfig(SCHEDULED_EXEC_NAME + quorumType.name() + postfix);
        config.setQuorumName(quorumName);
        return config;
    }

    protected static SetConfig newSetConfig(QuorumType quorumType, String quorumName) {
        SetConfig config = new SetConfig(SET_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        return config;
    }

    protected static PNCounterConfig newPNCounterConfig(QuorumType quorumType, String quorumName) {
        PNCounterConfig config = new PNCounterConfig(PN_COUNTER_NAME + quorumType.name());
        config.setQuorumName(quorumName);
        return config;
    }

    protected static QuorumConfig newQuorumConfig(QuorumType quorumType, String quorumName) {
        QuorumConfig quorumConfig = new QuorumConfig();
        quorumConfig.setName(quorumName);
        quorumConfig.setType(quorumType);
        quorumConfig.setEnabled(true);
        quorumConfig.setSize(3);
        return quorumConfig;
    }

    protected static void initCluster(Config config, TestHazelcastInstanceFactory factory, QuorumType... types) {
        cluster = new PartitionedCluster(factory);

        String[] quorumNames = new String[types.length];
        int i = 0;
        for (QuorumType quorumType : types) {
            String quorumName = QUORUM_ID + quorumType.name();
            config.addQuorumConfig(newQuorumConfig(quorumType, quorumName));
            quorumNames[i++] = quorumName;

            config.addSemaphoreConfig(newSemaphoreConfig(quorumType, quorumName));
            config.addAtomicReferenceConfig(newAtomicReferenceConfig(quorumType, quorumName));
            config.addAtomicLongConfig(newAtomicLongConfig(quorumType, quorumName));
            config.addCacheConfig(newCacheConfig(quorumType, quorumName));
            config.addCardinalityEstimatorConfig(newEstimatorConfig(quorumType, quorumName));
            config.addCountDownLatchConfig(newLatchConfig(quorumType, quorumName));
            config.addListConfig(newListConfig(quorumType, quorumName));
            config.addLockConfig(newLockConfig(quorumType, quorumName));
            config.addMapConfig(newMapConfig(quorumType, quorumName));
            config.addMultiMapConfig(newMultiMapConfig(quorumType, quorumName));
            config.addQueueConfig(newQueueConfig(quorumType, quorumName));
            config.addReplicatedMapConfig(newReplicatedMapConfig(quorumType, quorumName));
            config.addRingBufferConfig(newRingbufferConfig(quorumType, quorumName));
            for (String postfix : asList("", "shutdown", "shutdownNow")) {
                config.addDurableExecutorConfig(newDurableExecConfig(quorumType, quorumName, postfix));
                config.addExecutorConfig(newExecConfig(quorumType, quorumName, postfix));
                config.addScheduledExecutorConfig(newScheduledExecConfig(quorumType, quorumName, postfix));
            }
            config.addSetConfig(newSetConfig(quorumType, quorumName));
            config.addPNCounterConfig(newPNCounterConfig(quorumType, quorumName));
        }

        cluster.createFiveMemberCluster(config);
        initData(types);
        cluster.splitFiveMembersThreeAndTwo(quorumNames);
    }

    private static void initData(QuorumType[] types) {
        for (QuorumType quorumType : types) {
            for (int element = 0; element < 10000; element++) {
                cluster.instance[0].getQueue(QUEUE_NAME + quorumType.name()).offer(element);
            }
            for (int id = 0; id < 10000; id++) {
                cluster.instance[0].getRingbuffer(RINGBUFFER_NAME + quorumType.name()).add(String.valueOf(id));
            }
            cluster.instance[0].getSemaphore(SEMAPHORE + quorumType.name()).init(100);
        }
    }

    protected ISemaphore semaphore(int index, QuorumType quorumType) {
        return cluster.instance[index].getSemaphore(SEMAPHORE + quorumType.name());
    }

    protected IAtomicReference<QuorumTestClass> aref(int index, QuorumType quorumType) {
        return cluster.instance[index].getAtomicReference(REFERENCE_NAME + quorumType.name());
    }

    protected IAtomicLong along(int index, QuorumType quorumType) {
        return cluster.instance[index].getAtomicLong(LONG_NAME + quorumType.name());
    }

    protected ICache<Integer, String> cache(int index, QuorumType quorumType) {
        return cluster.instance[index].getCacheManager().getCache(CACHE_NAME + quorumType.name());
    }

    protected CardinalityEstimator estimator(int index, QuorumType quorumType) {
        return cluster.instance[index].getCardinalityEstimator(ESTIMATOR_NAME + quorumType.name());
    }

    protected ICountDownLatch latch(int index, QuorumType quorumType) {
        return cluster.instance[index].getCountDownLatch(LATCH_NAME + quorumType.name());
    }

    protected DurableExecutorService durableExec(int index, QuorumType quorumType) {
        return durableExec(index, quorumType, "");
    }

    protected DurableExecutorService durableExec(int index, QuorumType quorumType, String postfix) {
        return cluster.instance[index].getDurableExecutorService(DURABLE_EXEC_NAME + quorumType.name() + postfix);
    }

    protected IExecutorService exec(int index, QuorumType quorumType) {
        return exec(index, quorumType, "");
    }

    protected IExecutorService exec(int index, QuorumType quorumType, String postfix) {
        return cluster.instance[index].getExecutorService(EXEC_NAME + quorumType.name() + postfix);
    }

    protected IList list(int index, QuorumType quorumType) {
        return cluster.instance[index].getList(LIST_NAME + quorumType.name());
    }

    protected ILock lock(int index, QuorumType quorumType) {
        return cluster.instance[index].getLock(LOCK_NAME + quorumType.name());
    }

    protected IMap map(int index, QuorumType quorumType) {
        return cluster.instance[index].getMap(MAP_NAME + quorumType.name());
    }

    protected MultiMap multimap(int index, QuorumType quorumType) {
        return cluster.instance[index].getMultiMap(MULTI_MAP_NAME + quorumType.name());
    }

    protected IQueue queue(int index, QuorumType quorumType) {
        return cluster.instance[index].getQueue(QUEUE_NAME + quorumType.name());
    }

    protected ReplicatedMap replmap(int index, QuorumType quorumType) {
        return cluster.instance[index].getReplicatedMap(REPLICATED_MAP_NAME + quorumType.name());
    }

    protected Ringbuffer ring(int index, QuorumType quorumType) {
        return cluster.instance[index].getRingbuffer(RINGBUFFER_NAME + quorumType.name());
    }

    protected IScheduledExecutorService scheduledExec(int index, QuorumType quorumType) {
        return scheduledExec(index, quorumType, "");
    }

    protected IScheduledExecutorService scheduledExec(int index, QuorumType quorumType, String postfix) {
        return cluster.instance[index].getScheduledExecutorService(SCHEDULED_EXEC_NAME + quorumType.name() + postfix);
    }

    protected ISet set(int index, QuorumType quorumType) {
        return cluster.instance[index].getSet(SET_NAME + quorumType.name());
    }

    protected PNCounter pnCounter(int index, QuorumType quorumType) {
        return cluster.instance[index].getPNCounter(PN_COUNTER_NAME + quorumType.name());
    }

    protected static IFunction function() {
        return new IFunction<Object, Object>() {
            @Override
            public Object apply(Object input) {
                return input;
            }
        };
    }

    public static class QuorumTestClass implements Serializable {

        public static QuorumTestClass object() {
            return new QuorumTestClass();
        }
    }
}
