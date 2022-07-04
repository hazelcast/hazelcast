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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.QueueStore;
import com.hazelcast.config.Config;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.collection.impl.CollectionTestUtil.getBackupQueue;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests different split-brain scenarios for {@link IQueue}.
 * <p>
 * The {@link DiscardMergePolicy}, {@link PassThroughMergePolicy} and {@link PutIfAbsentMergePolicy} are also
 * tested with a data structure, which is only created in the smaller cluster.
 * <p>
 * The number and content of backup items are tested for all merge policies.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueSplitBrainTest extends SplitBrainTestSupport {

    private static final int ITEM_COUNT = 25;

    @Parameters(name = "mergePolicy:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                DiscardMergePolicy.class,
                PassThroughMergePolicy.class,
                PutIfAbsentMergePolicy.class,
                RemoveValuesMergePolicy.class,
                ReturnPiCollectionMergePolicy.class,
                MergeCollectionOfIntegerValuesMergePolicy.class,
        });
    }

    @Parameter
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    private String queueNameA = randomMapName("QueueA-");
    private String queueNameB = randomMapName("QueueB-");
    private SplitBrainQueueStore queueStoreA = new SplitBrainQueueStore();
    private SplitBrainQueueStore queueStoreB = new SplitBrainQueueStore();
    private IQueue<Object> queueA1;
    private IQueue<Object> queueA2;
    private IQueue<Object> queueB1;
    private IQueue<Object> queueB2;
    private Queue<Object> backupQueue;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getQueueConfig(queueNameA)
                .setMergePolicyConfig(mergePolicyConfig)
                .setQueueStoreConfig(new QueueStoreConfig()
                        .setStoreImplementation(queueStoreA))
                .setBackupCount(1)
                .setAsyncBackupCount(0);
        config.getQueueConfig(queueNameB)
                .setMergePolicyConfig(mergePolicyConfig)
                .setQueueStoreConfig(new QueueStoreConfig()
                        .setStoreImplementation(queueStoreB))
                .setBackupCount(1)
                .setAsyncBackupCount(0);
        return config;
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        queueA1 = firstBrain[0].getQueue(queueNameA);
        queueA2 = secondBrain[0].getQueue(queueNameA);

        queueB2 = secondBrain[0].getQueue(queueNameB);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterSplitDiscardMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterSplitPassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            afterSplitPutIfAbsentMergePolicy();
        } else if (mergePolicyClass == RemoveValuesMergePolicy.class) {
            afterSplitRemoveValuesMergePolicy();
        } else if (mergePolicyClass == ReturnPiCollectionMergePolicy.class) {
            afterSplitReturnPiCollectionMergePolicy();
        } else if (mergePolicyClass == MergeCollectionOfIntegerValuesMergePolicy.class) {
            afterSplitCustomMergePolicy();
        } else {
            fail();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        // we manually purge the unwanted items from the QueueStore, to test if the expected items are correctly stored
        queueStoreA.purgeValuesWithPrefix("lostItem");
        queueStoreB.purgeValuesWithPrefix("lostItem");

        backupQueue = getBackupQueue(instances, queueA1);

        queueB1 = instances[0].getQueue(queueNameB);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterMergeDiscardMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterMergePassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            afterMergePutIfAbsentMergePolicy();
        } else if (mergePolicyClass == RemoveValuesMergePolicy.class) {
            afterMergeRemoveValuesMergePolicy();
        } else if (mergePolicyClass == ReturnPiCollectionMergePolicy.class) {
            afterMergeReturnPiCollectionMergePolicy();
        } else if (mergePolicyClass == MergeCollectionOfIntegerValuesMergePolicy.class) {
            afterMergeCustomMergePolicy();
        } else {
            fail();
        }
    }

    private void afterSplitDiscardMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            queueA1.add("item" + i);
            queueA2.add("lostItem" + i);

            queueB2.add("lostItem" + i);
        }
    }

    private void afterMergeDiscardMergePolicy() {
        assertQueueContent(queueA1);
        assertQueueContent(queueA2);
        assertQueueContent(backupQueue);
        assertQueueStoreContent(queueStoreA);

        assertQueueContent(queueB1, 0);
        assertQueueContent(queueB2, 0);
        assertQueueStoreContent(queueStoreB, 0);
    }

    private void afterSplitPassThroughMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            queueA1.add("lostItem" + i);
            queueA2.add("item" + i);

            queueB2.add("item" + i);
        }
    }

    private void afterMergePassThroughMergePolicy() {
        assertQueueContent(queueA1);
        assertQueueContent(queueA2);
        assertQueueContent(backupQueue);
        assertQueueStoreContent(queueStoreA);

        assertQueueContent(queueB1);
        assertQueueContent(queueB2);
        assertQueueStoreContent(queueStoreB);
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            queueA1.add("item" + i);
            queueA2.add("lostItem" + i);

            queueB2.add("item" + i);
        }
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertQueueContent(queueA1);
        assertQueueContent(queueA2);
        assertQueueContent(backupQueue);
        assertQueueStoreContent(queueStoreA);

        assertQueueContent(queueB1);
        assertQueueContent(queueB2);
        assertQueueStoreContent(queueStoreB);
    }

    private void afterSplitRemoveValuesMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            queueA1.add("lostItem" + i);
            queueA2.add("lostItem" + i);

            queueB2.add("lostItem" + i);
        }
    }

    private void afterMergeRemoveValuesMergePolicy() {
        assertQueueContent(queueA1, 0);
        assertQueueContent(queueA2, 0);
        assertQueueContent(backupQueue, 0);
        assertQueueStoreContent(queueStoreA, 0);

        assertQueueContent(queueB1, 0);
        assertQueueContent(queueB2, 0);
        assertQueueStoreContent(queueStoreB, 0);
    }

    private void afterSplitReturnPiCollectionMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            queueA1.add("lostItem" + i);
            queueA2.add("lostItem" + i);

            queueB2.add("lostItem" + i);
        }
    }

    private void afterMergeReturnPiCollectionMergePolicy() {
        assertPiCollection(queueA1);
        assertPiCollection(queueA2);
        assertPiCollection(backupQueue);

        assertPiCollection(queueB1);
        assertPiCollection(queueB2);
    }

    private void afterSplitCustomMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            queueA2.add(i);
            queueA2.add("lostItem" + i);
        }
        // we clear the QueueStore, so we can check that the custom created items are correctly stored
        queueStoreA.clear();
    }

    private void afterMergeCustomMergePolicy() {
        assertQueueContent(queueA1, ITEM_COUNT);
        assertQueueContent(queueA2, ITEM_COUNT);
        assertQueueContent(backupQueue, ITEM_COUNT);
        assertQueueStoreContent(queueStoreA, ITEM_COUNT);
    }

    private static void assertQueueContent(Queue<Object> queue) {
        assertQueueContent(queue, ITEM_COUNT, "item");
    }

    private static void assertQueueContent(Queue<Object> queue, int expectedSize) {
        assertQueueContent(queue, expectedSize, null);
    }

    private static void assertQueueContent(Queue<Object> queue, int expectedSize, String prefix) {
        assertEqualsStringFormat("queue " + toString(queue) + " should contain %d items, but was %d", expectedSize, queue.size());

        for (int i = 0; i < expectedSize; i++) {
            Object expectedValue = prefix == null ? i : prefix + i;
            assertTrue("queue " + toString(queue) + " should contain " + expectedValue, queue.contains(expectedValue));
        }
    }

    private static void assertQueueStoreContent(SplitBrainQueueStore queueStore) {
        assertQueueStoreContent(queueStore, ITEM_COUNT, "item");
    }

    private static void assertQueueStoreContent(SplitBrainQueueStore queueStore, int expectedSize) {
        assertQueueStoreContent(queueStore, expectedSize, null);
    }

    private static void assertQueueStoreContent(SplitBrainQueueStore queueStore, int expectedSize, String prefix) {
        assertEqualsStringFormat("queueStore " + queueStore + " should contain %d items, but was %d",
                expectedSize, queueStore.size());

        for (int i = 0; i < expectedSize; i++) {
            Object expectedValue = prefix == null ? i : prefix + i;
            assertTrue("queueStore " + queueStore + " should contain " + expectedValue, queueStore.contains(expectedValue));
        }
    }

    /**
     * A {@link QueueStore} implementation for split-brain tests.
     * <p>
     * <b>Note</b>: The {@link QueueStore} uses the internal item ID from the {@link QueueItem}.
     * This ID is not reliable during a split-brain situation, since there can be duplicates in each sub-cluster.
     * Also the order is not guaranteed to be the same between the sub-clusters.
     * So after the split-brain healing we cannot make a strict test on the stored items.
     * The split-brain healing also doesn't try to delete any old items, but adds newly created items to the store.
     */
    private static class SplitBrainQueueStore implements QueueStore<Object> {

        private final ConcurrentMap<Long, Collection<Object>> store = new ConcurrentHashMap<Long, Collection<Object>>();

        @Override
        public Object load(Long key) {
            return null;
        }

        @Override
        public Map<Long, Object> loadAll(Collection<Long> keys) {
            return emptyMap();
        }

        @Override
        public Set<Long> loadAllKeys() {
            return emptySet();
        }

        @Override
        public void store(Long key, Object value) {
            Collection<Object> collection = getCollection(key);
            collection.add(value);
        }

        @Override
        public void storeAll(Map<Long, Object> map) {
            for (Map.Entry<Long, Object> entry : map.entrySet()) {
                Collection<Object> collection = getCollection(entry.getKey());
                collection.add(entry.getValue());
            }
        }

        @Override
        public void delete(Long key) {
            store.remove(key);
        }

        @Override
        public void deleteAll(Collection<Long> keys) {
            for (Long key : keys) {
                store.remove(key);
            }
        }

        @Override
        public String toString() {
            return store.toString();
        }

        void clear() {
            store.clear();
        }

        @SuppressWarnings("SameParameterValue")
        void purgeValuesWithPrefix(String prefix) {
            Iterator<Collection<Object>> iterator = store.values().iterator();
            while (iterator.hasNext()) {
                Collection<Object> collection = iterator.next();
                Iterator<Object> collectionIterator = collection.iterator();
                while (collectionIterator.hasNext()) {
                    Object value = collectionIterator.next();
                    if (value instanceof String) {
                        if (((String) value).startsWith(prefix)) {
                            collectionIterator.remove();
                        }
                    }
                }
                if (collection.isEmpty()) {
                    iterator.remove();
                }
            }
        }

        int size() {
            return store.size();
        }

        boolean contains(Object expectedValue) {
            for (Collection<Object> collection : store.values()) {
                if (collection.contains(expectedValue)) {
                    return true;
                }
            }
            return false;
        }

        private Collection<Object> getCollection(Long key) {
            Collection<Object> collection = store.get(key);
            if (collection == null) {
                collection = new ConcurrentLinkedQueue<Object>();
                Collection<Object> candidate = store.putIfAbsent(key, collection);
                if (candidate != null) {
                    return candidate;
                }
            }
            return collection;
        }
    }
}
