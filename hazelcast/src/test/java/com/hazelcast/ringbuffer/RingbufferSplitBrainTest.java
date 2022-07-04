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

package com.hazelcast.ringbuffer;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.ringbuffer.RingbufferTestUtil.getBackupRingbuffer;
import static com.hazelcast.test.Accessors.getSerializationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests different split-brain scenarios for {@link Ringbuffer}.
 * <p>
 * The {@link DiscardMergePolicy}, {@link PassThroughMergePolicy} and {@link PutIfAbsentMergePolicy} are also
 * tested with a data structure, which is only created in the smaller cluster.
 * <p>
 * The number and content of backup items are tested for all merge policies.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferSplitBrainTest extends SplitBrainTestSupport {

    private static final int ITEM_COUNT = 25;

    @Parameters(name = "format:{0}, mergePolicy:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY, DiscardMergePolicy.class},
                {BINARY, PassThroughMergePolicy.class},
                {BINARY, PutIfAbsentMergePolicy.class},

                {BINARY, RingbufferMergeIntegerValuesMergePolicy.class},
                {OBJECT, RingbufferMergeIntegerValuesMergePolicy.class},

                {BINARY, RingbufferRemoveValuesMergePolicy.class},
                {OBJECT, RingbufferRemoveValuesMergePolicy.class},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    private String ringbufferNameA = randomMapName("ringbufferA-");
    private String ringbufferNameB = randomMapName("ringbufferB-");
    private SplitBrainRingbufferStore ringbufferStoreA = new SplitBrainRingbufferStore().setLabel("A");
    private SplitBrainRingbufferStore ringbufferStoreB = new SplitBrainRingbufferStore().setLabel("B");
    private Ringbuffer<Object> ringbufferA1;
    private Ringbuffer<Object> ringbufferA2;
    private Ringbuffer<Object> ringbufferB1;
    private Ringbuffer<Object> ringbufferB2;
    private Collection<Object> backupRingbuffer;
    private MergeLifecycleListener mergeLifecycleListener;
    private InternalSerializationService serializationService;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(mergePolicyClass.getName())
                .setBatchSize(10);

        Config config = super.config();
        config.getRingbufferConfig(ringbufferNameA)
                .setInMemoryFormat(inMemoryFormat)
                .setMergePolicyConfig(mergePolicyConfig)
                .setRingbufferStoreConfig(new RingbufferStoreConfig()
                        .setStoreImplementation(ringbufferStoreA))
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setTimeToLiveSeconds(0);
        config.getRingbufferConfig(ringbufferNameB)
                .setInMemoryFormat(inMemoryFormat)
                .setMergePolicyConfig(mergePolicyConfig)
                .setRingbufferStoreConfig(new RingbufferStoreConfig()
                        .setStoreImplementation(ringbufferStoreB))
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setTimeToLiveSeconds(0);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        serializationService = getSerializationService(instances[0]);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        ringbufferA1 = firstBrain[0].getRingbuffer(ringbufferNameA);
        ringbufferA2 = secondBrain[0].getRingbuffer(ringbufferNameA);

        ringbufferB2 = secondBrain[0].getRingbuffer(ringbufferNameB);

        ringbufferA1.size();
        ringbufferA2.size();
        ringbufferB2.size();

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterSplitDiscardMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterSplitPassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            afterSplitPutIfAbsentMergePolicy();
        } else if (mergePolicyClass == RingbufferRemoveValuesMergePolicy.class) {
            afterSplitRemoveValuesMergePolicy();
        } else if (mergePolicyClass == RingbufferMergeIntegerValuesMergePolicy.class) {
            afterSplitCustomMergePolicy();
        } else {
            fail();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        // we manually purge the unwanted items from the RingbufferStore, to test if the expected items are correctly stored
        ringbufferStoreA.purge("lostItem");
        ringbufferStoreB.purge("lostItem");

        backupRingbuffer = getBackupRingbuffer(instances, ringbufferA1);

        ringbufferB1 = instances[0].getRingbuffer(ringbufferNameB);

        if (mergePolicyClass == DiscardMergePolicy.class) {
            afterMergeDiscardMergePolicy();
        } else if (mergePolicyClass == PassThroughMergePolicy.class) {
            afterMergePassThroughMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentMergePolicy.class) {
            afterMergePutIfAbsentMergePolicy();
        } else if (mergePolicyClass == RingbufferRemoveValuesMergePolicy.class) {
            afterMergeRemoveValuesMergePolicy();
        } else if (mergePolicyClass == RingbufferMergeIntegerValuesMergePolicy.class) {
            afterMergeCustomMergePolicy();
        } else {
            fail();
        }
    }

    private void afterSplitDiscardMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            ringbufferA1.add("item" + i);
            ringbufferA2.add("lostItem" + i);

            ringbufferB2.add("lostItem" + i);
        }
    }

    private void afterMergeDiscardMergePolicy() {
        assertRingbufferContent(ringbufferA1);
        assertRingbufferContent(ringbufferA2);
        assertRingbufferContent(backupRingbuffer);
        assertRingbufferStoreContent(ringbufferStoreA);

        assertRingbufferContent(ringbufferB1, 0);
        assertRingbufferContent(ringbufferB2, 0);
        assertRingbufferStoreContent(ringbufferStoreB, 0);
    }

    private void afterSplitPassThroughMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            ringbufferA1.add("lostItem" + i);
            ringbufferA2.add("item" + i);

            ringbufferB2.add("item" + i);
        }
    }

    private void afterMergePassThroughMergePolicy() {
        assertRingbufferContent(ringbufferA1);
        assertRingbufferContent(ringbufferA2);
        assertRingbufferContent(backupRingbuffer);
        assertRingbufferStoreContent(ringbufferStoreA);

        assertRingbufferContent(ringbufferB1);
        assertRingbufferContent(ringbufferB2);
        assertRingbufferStoreContent(ringbufferStoreB);
    }

    private void afterSplitPutIfAbsentMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            ringbufferA1.add("item" + i);
            ringbufferA2.add("lostItem" + i);

            ringbufferB2.add("item" + i);
        }
    }

    private void afterMergePutIfAbsentMergePolicy() {
        assertRingbufferContent(ringbufferA1);
        assertRingbufferContent(ringbufferA2);
        assertRingbufferContent(backupRingbuffer);
        assertRingbufferStoreContent(ringbufferStoreA);

        assertRingbufferContent(ringbufferB1);
        assertRingbufferContent(ringbufferB2);
        assertRingbufferStoreContent(ringbufferStoreB);
    }

    private void afterSplitRemoveValuesMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            ringbufferA1.add("lostItem" + i);
            ringbufferA2.add("lostItem" + i);

            ringbufferB2.add("lostItem" + i);
        }
    }

    private void afterMergeRemoveValuesMergePolicy() {
        assertRingbufferContent(ringbufferA1, 0);
        assertRingbufferContent(ringbufferA2, 0);
        assertRingbufferContent(backupRingbuffer, 0);
        assertRingbufferStoreContent(ringbufferStoreA, 0);

        assertRingbufferContent(ringbufferB1, 0);
        assertRingbufferContent(ringbufferB2, 0);
        assertRingbufferStoreContent(ringbufferStoreB, 0);
    }

    private void afterSplitCustomMergePolicy() {
        for (int i = 0; i < ITEM_COUNT; i++) {
            ringbufferA2.add(i);
            ringbufferA2.add("lostItem" + i);
        }
        // we clear the RingbufferStore, so we can check that the custom created items are correctly stored
        ringbufferStoreA.store.clear();
    }

    private void afterMergeCustomMergePolicy() {
        assertRingbufferContent(ringbufferA1, ITEM_COUNT);
        assertRingbufferContent(ringbufferA2, ITEM_COUNT);
        assertRingbufferContent(backupRingbuffer, ITEM_COUNT);
        assertRingbufferStoreContent(ringbufferStoreA, ITEM_COUNT);
    }

    private void assertRingbufferStoreContent(SplitBrainRingbufferStore ringbufferStore) {
        assertRingbufferStoreContent(ringbufferStore, ITEM_COUNT, "item");
    }

    private void assertRingbufferStoreContent(SplitBrainRingbufferStore ringbufferStore, int expectedSize) {
        assertRingbufferStoreContent(ringbufferStore, expectedSize, null);
    }

    private void assertRingbufferStoreContent(SplitBrainRingbufferStore ringbufferStore, int expectedSize, String prefix) {
        assertEqualsStringFormat("ringbufferStore" + ringbufferStore.label + " should contain %d items, but was %d",
                expectedSize, ringbufferStore.size());

        for (int i = 0; i < expectedSize; i++) {
            Object expectedValue = prefix == null ? i : prefix + i;
            if (inMemoryFormat == BINARY) {
                expectedValue = serializationService.toData(expectedValue).toByteArray();
            }
            assertTrue("ringbufferStore" + ringbufferStore.label + " should contain " + expectedValue,
                    ringbufferStore.contains(expectedValue));
        }
    }

    private static void assertRingbufferContent(Ringbuffer<Object> ringbuffer, int expectedSize) {
        assertRingbufferContent(getRingbufferContent(ringbuffer), expectedSize);
    }

    private static void assertRingbufferContent(Collection<Object> ringbuffer, int expectedSize) {
        assertRingbufferContent(ringbuffer, expectedSize, null);
    }

    private static void assertRingbufferContent(Ringbuffer<Object> ringbuffer) {
        assertRingbufferContent(getRingbufferContent(ringbuffer));
    }

    private static void assertRingbufferContent(Collection<Object> ringbuffer) {
        assertRingbufferContent(ringbuffer, ITEM_COUNT, "item");
    }

    private static void assertRingbufferContent(Collection<Object> ringbuffer, int expectedSize, String prefix) {
        assertEqualsStringFormat("ringbuffer " + toString(ringbuffer) + " should contain %d items, but was %d ",
                expectedSize, ringbuffer.size());

        for (int i = 0; i < expectedSize; i++) {
            Object expectedValue = prefix == null ? i : prefix + i;
            assertTrue("ringbuffer " + toString(ringbuffer) + " should contain " + expectedValue,
                    ringbuffer.contains(expectedValue));
        }
    }

    private static Collection<Object> getRingbufferContent(Ringbuffer<Object> ringbuffer) {
        List<Object> list = new LinkedList<Object>();
        try {
            for (long sequence = ringbuffer.headSequence(); sequence <= ringbuffer.tailSequence(); sequence++) {
                list.add(ringbuffer.readOne(sequence));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return list;
    }

    /**
     * A {@link RingbufferStore} implementation for split-brain tests.
     *
     * <b>Note</b>: The {@link RingbufferStore} uses the sequence ID of the ringbuffer item.
     * This ID is not reliable during a split-brain situation, since there can be duplicates in each sub-cluster.
     * Also the order is not guaranteed to be the same between the sub-clusters.
     * So after the split-brain healing we cannot make a strict test on the stored items.
     * The split-brain healing also doesn't try to delete any old items, but adds newly created items to the store.
     */
    private static class SplitBrainRingbufferStore implements RingbufferStore<Object> {

        private final SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        private final ConcurrentMap<Long, Collection<Object>> store = new ConcurrentHashMap<Long, Collection<Object>>();

        private String label;

        SplitBrainRingbufferStore() {
        }

        SplitBrainRingbufferStore setLabel(String label) {
            this.label = label;
            return this;
        }

        @Override
        public void store(long sequence, Object data) {
            Collection<Object> collection = getCollection(sequence);
            collection.add(data);
        }

        @Override
        public void storeAll(long firstItemSequence, Object[] items) {
            for (Object item : items) {
                Collection<Object> collection = getCollection(firstItemSequence++);
                collection.add(item);
            }
        }

        @Override
        public Object load(long sequence) {
            return null;
        }

        @Override
        public long getLargestSequence() {
            long maxSeq = -1;
            for (Long seq : store.keySet()) {
                maxSeq = Math.max(seq, maxSeq);
            }
            return maxSeq;
        }

        @SuppressWarnings("SameParameterValue")
        void purge(String prefix) {
            Iterator<Collection<Object>> iterator = store.values().iterator();
            while (iterator.hasNext()) {
                Collection<Object> collection = iterator.next();
                Iterator<Object> collectionIterator = collection.iterator();
                while (collectionIterator.hasNext()) {
                    Object value = collectionIterator.next();
                    if (value instanceof byte[]) {
                        // binary in-memory format and store format
                        value = serializationService.toObject(new HeapData((byte[]) value));
                    }
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
                if (expectedValue instanceof byte[]) {
                    // binary in-memory format and store format
                    for (Object storedItem : collection) {
                        if (Arrays.equals((byte[]) storedItem, (byte[]) expectedValue)) {
                            return true;
                        }
                    }
                } else {
                    if (collection.contains(expectedValue)) {
                        return true;
                    }
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
