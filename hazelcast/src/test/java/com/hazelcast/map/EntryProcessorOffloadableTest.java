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

package com.hazelcast.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Offloadable;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.InvocationMonitor;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryProcessorOffloadableTest extends HazelcastTestSupport {

    public static final String MAP_NAME = "EntryProcessorOffloadableTest";

    private HazelcastInstance[] instances;

    @Parameter(0)
    public InMemoryFormat inMemoryFormat;

    @Parameter(1)
    public int syncBackupCount;

    @Parameter(2)
    public int asyncBackupCount;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Parameters(name = "{index}: {0} sync={1} async={2}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {BINARY, 0, 0}, {OBJECT, 0, 0},
                {BINARY, 1, 0}, {OBJECT, 1, 0},
                {BINARY, 0, 1}, {OBJECT, 0, 1},
        });
    }

    private boolean isBackup() {
        return syncBackupCount + asyncBackupCount > 0;
    }

    @Override
    public Config getConfig() {
        Config config = smallInstanceConfig();
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setInMemoryFormat(inMemoryFormat);
        mapConfig.setAsyncBackupCount(asyncBackupCount);
        mapConfig.setBackupCount(syncBackupCount);
        config.addMapConfig(mapConfig);
        return config;
    }

    @Before
    public void before() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        instances = factory.newInstances(getConfig());
    }

    @Test
    public void testEntryProcessorWithKey_offloadable_setValue() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        SimpleValue expectedValue = new SimpleValue(2);

        IMap<String, SimpleValue> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);
        Integer result = map.executeOnKey(key, new EntryIncOffloadable());

        assertEquals(expectedValue, map.get(key));
        assertBackupEventually(instances[1], MAP_NAME, key, isBackup() ? expectedValue : null);
        assertEquals(givenValue.i, (int) result);


        instances[0].shutdown();
        assertEquals(expectedValue, map.get(key));
        assertEquals(givenValue.i, (int) result);
    }

    @Test
    public void testEntryProcessorWithKey_offloadable_withoutSetValue() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        SimpleValue expectedValue = new SimpleValue(1);

        IMap<String, SimpleValue> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);
        Integer result = map.executeOnKey(key, new EntryIncOffloadableNoSetValue());

        assertEquals(expectedValue, map.get(key));
        if (inMemoryFormat.equals(OBJECT)) {
            assertBackupEventually(instances[1], MAP_NAME, key, isBackup() ? new SimpleValue(2) : null);
        } else {
            assertBackupEventually(instances[1], MAP_NAME, key, isBackup() ? expectedValue : null);
        }
        assertEquals(givenValue.i, (int) result);

        instances[0].shutdown();
        assertEquals(expectedValue, map.get(key));
    }

    private static class EntryIncOffloadable implements EntryProcessor<String, SimpleValue, Integer>, Offloadable {
        @Override
        public Integer process(final Map.Entry<String, SimpleValue> entry) {
            final SimpleValue value = entry.getValue();
            int result = value.i;
            value.i++;
            entry.setValue(value);
            return result;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }
    }

    private static class EntryIncOffloadableNoSetValue implements EntryProcessor<String, SimpleValue, Integer>, Offloadable {
        @Override
        public Integer process(final Map.Entry<String, SimpleValue> entry) {
            final SimpleValue value = entry.getValue();
            int result = value.i;
            value.i++;
            return result;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }
    }

    @Test
    public void testEntryProcessorWithKey_offloadableReadOnly_setValue() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);

        IMap<String, SimpleValue> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);

        expectedException.expect(UnsupportedOperationException.class);

        map.executeOnKey(key, new EntryIncOffloadableReadOnly());
    }

    private static class EntryIncOffloadableReadOnly implements EntryProcessor<String, SimpleValue, Integer>, Offloadable, ReadOnly {
        @Override
        public Integer process(final Map.Entry<String, SimpleValue> entry) {
            final SimpleValue value = entry.getValue();
            int result = value.i;
            value.i++;
            entry.setValue(value);
            return result;
        }

        @Override
        public EntryProcessor<String, SimpleValue, Integer> getBackupProcessor() {
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }
    }


    @Test
    public void testEntryProcessorWithKey_offloadableReadOnly_withoutSetValue() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        SimpleValue expectedValue = new SimpleValue(1);

        IMap<String, SimpleValue> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);
        Object result = map.executeOnKey(key, new EntryIncOffloadableReadOnlyNoSetValue());

        assertEquals(expectedValue, map.get(key));
        assertBackupEventually(instances[1], MAP_NAME, key, isBackup() ? expectedValue : null);
        assertEquals(givenValue.i, result);

        instances[0].shutdown();
        assertEquals(expectedValue, map.get(key));
    }

    private static class EntryIncOffloadableReadOnlyNoSetValue
            implements EntryProcessor<String, SimpleValue, Integer>, Offloadable, ReadOnly {
        @Override
        public Integer process(final Map.Entry<String, SimpleValue> entry) {
            final SimpleValue value = entry.getValue();
            int result = value.i;
            value.i++;
            return result;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public EntryProcessor<String, SimpleValue, Integer> getBackupProcessor() {
            return null;
        }
    }

    @Test
    public void testEntryProcessorWithKey_offloadableReadOnly_returnValue() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        SimpleValue expectedValue = new SimpleValue(1);

        IMap<String, SimpleValue> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);
        Integer result = map.executeOnKey(key, new EntryIncOffloadableReadOnlyReturnValue());

        assertEquals(expectedValue, map.get(key));
        assertBackupEventually(instances[1], MAP_NAME, key, isBackup() ? expectedValue : null);
        assertEquals(givenValue.i, (int) result);

        instances[0].shutdown();
        assertEquals(expectedValue, map.get(key));
    }

    private static class EntryIncOffloadableReadOnlyReturnValue
            implements EntryProcessor<String, SimpleValue, Integer>, Offloadable, ReadOnly {
        @Override
        public Integer process(final Map.Entry<String, SimpleValue> entry) {
            final SimpleValue value = entry.getValue();
            return value.i;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public EntryProcessor<String, SimpleValue, Integer> getBackupProcessor() {
            return null;
        }
    }


    @Test
    public void testEntryProcessorWithKey_offloadableReadOnly_noReturnValue() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        SimpleValue expectedValue = new SimpleValue(1);

        IMap<String, SimpleValue> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);
        Object result = map.executeOnKey(key, new EntryIncOffloadableReadOnlyNoReturnValue());
        assertEquals(expectedValue, map.get(key));
        assertBackupEventually(instances[1], MAP_NAME, key, isBackup() ? expectedValue : null);
        assertNull(result);

        instances[0].shutdown();
        assertEquals(expectedValue, map.get(key));
    }

    private static class EntryIncOffloadableReadOnlyNoReturnValue
            implements EntryProcessor<String, SimpleValue, Object>, Offloadable, ReadOnly {
        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public EntryProcessor<String, SimpleValue, Object> getBackupProcessor() {
            return null;
        }
    }

    @Test
    public void testEntryProcessorWithKey_offloadableReadOnly_throwsException() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);

        IMap<String, SimpleValue> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);

        expectedException.expect(RuntimeException.class);
        map.executeOnKey(key, new EntryIncOffloadableReadOnlyException());
        assertFalse(map.isLocked(key));
    }

    private static class EntryIncOffloadableReadOnlyException
            implements EntryProcessor<String, SimpleValue, Object>, Offloadable, ReadOnly {
        @Override
        public Object process(Map.Entry<String, SimpleValue> entry) {
            throw new RuntimeException("EP exception");
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public EntryProcessor<String, SimpleValue, Object> getBackupProcessor() {
            return null;
        }
    }

    @Test
    public void testEntryProcessorWithKey_offloadableModifying_throwsException_keyNotLocked() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);

        IMap<String, SimpleValue> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);

        expectedException.expect(RuntimeException.class);
        map.executeOnKey(key, new EntryIncOffloadableException());

        assertFalse(map.isLocked(key));
    }

    @Test
    public void puts_multiple_entry_into_empty_map() {
        IMap<Integer, Integer> map = instances[1].getMap(MAP_NAME);

        Set<Integer> keySet = new HashSet<>(asList(1, 2, 3, 4, 5));

        Integer value = -1;
        map.executeOnKeys(keySet, new EntryAdderOffloadable<>(value));

        for (Integer key : keySet) {
            assertEquals(value, map.get(key));
        }
    }

    @Test
    public void puts_entry_into_empty_map() {
        IMap<Integer, Integer> map = instances[1].getMap(MAP_NAME);

        Integer key = 1;
        Integer value = -1;

        map.executeOnKey(key, new EntryAdderOffloadable<>(value));

        assertEquals(value, map.get(key));
    }

    private static class EntryIncOffloadableException implements EntryProcessor<String, SimpleValue, Object>, Offloadable {
        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            throw new RuntimeException("EP exception");
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public EntryProcessor<String, SimpleValue, Object> getBackupProcessor() {
            return null;
        }
    }

    private static class EntryAdderOffloadable<K, V, R> implements EntryProcessor<K, V, R>, Offloadable {

        private final V value;

        EntryAdderOffloadable(V value) {
            this.value = value;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public R process(Map.Entry<K, V> entry) {
            entry.setValue(value);
            return null;
        }
    }


    void assertBackupEventually(final HazelcastInstance instance, final String mapName, final Object key, Object expected) {
        assertEqualsEventually(() -> readFromMapBackup(instance, mapName, key), expected);
    }

    @Test
    public void testEntryProcessorWithKey_offloadable_otherModifyingWillWait() throws InterruptedException {
        final String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        SimpleValue expectedValue = new SimpleValue(4);

        final IMap<String, SimpleValue> map = instances[0].getMap(MAP_NAME);
        map.put(key, givenValue);

        final CountDownLatch epStarted = new CountDownLatch(1);
        final CountDownLatch epStopped = new CountDownLatch(1);
        new Thread(() -> map.executeOnKey(key, new EntryLatchModifying(epStarted, epStopped))).start();

        epStarted.await();
        map.executeOnKey(key, new EntryLatchVerifying(epStarted, epStopped, 4));

        // verified EPs not out-of-order, and not at the same time
        assertEqualsEventually((Callable<Object>) () -> map.get(key), expectedValue);
    }

    private static class EntryLatchModifying implements EntryProcessor<String, SimpleValue, Object>, Offloadable {

        private final CountDownLatch start;
        private final CountDownLatch stop;

        EntryLatchModifying(CountDownLatch start, CountDownLatch stop) {
            this.start = start;
            this.stop = stop;
        }

        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            start.countDown();
            try {
                final SimpleValue value = entry.getValue();
                value.i++;
                entry.setValue(value);
                return null;
            } finally {
                stop.countDown();
            }
        }

        @Override
        public EntryProcessor<String, SimpleValue, Object> getBackupProcessor() {
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

    }

    private static class EntryLatchVerifying implements EntryProcessor<String, SimpleValue, Object>, Offloadable {

        private final CountDownLatch otherStarted;
        private final CountDownLatch otherStopped;
        private final int valueToSet;

        EntryLatchVerifying(CountDownLatch otherStarted, CountDownLatch otherStopped, final int value) {
            this.otherStarted = otherStarted;
            this.otherStopped = otherStopped;
            this.valueToSet = value;
        }

        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            if (otherStarted.getCount() != 0 || otherStopped.getCount() != 0) {
                throw new RuntimeException("Wrong threading order");
            }

            final SimpleValue value = entry.getValue();
            value.i = valueToSet;
            entry.setValue(value);
            return null;
        }

        @Override
        public EntryProcessor<String, SimpleValue, Object> getBackupProcessor() {
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }
    }

    @Test
    public void testEntryProcessorWithKey_offloadable_otherReadingWillNotWait() throws InterruptedException {
        final String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        SimpleValue expectedValue = new SimpleValue(2);

        final IMap<String, SimpleValue> map = instances[0].getMap(MAP_NAME);
        map.put(key, givenValue);

        final CountDownLatch epStarted = new CountDownLatch(1);
        final CountDownLatch epWaitToProceed = new CountDownLatch(1);
        final CountDownLatch epStopped = new CountDownLatch(1);
        new Thread(() -> map.executeOnKey(key, new EntryLatchModifyingOtherReading(epStarted, epWaitToProceed, epStopped))).start();

        epStarted.await();
        map.executeOnKey(key, new EntryLatchReadOnlyVerifyingWhileOtherWriting(epStarted, epWaitToProceed, epStopped));
        epStopped.await();

        // verified EPs not out-of-order, and not at the same time
        assertEqualsEventually(() -> map.get(key), expectedValue);
    }

    private static class EntryLatchModifyingOtherReading implements EntryProcessor<String, SimpleValue, Object>, Offloadable {

        private final CountDownLatch start;
        private final CountDownLatch stop;
        private final CountDownLatch waitToProceed;

        EntryLatchModifyingOtherReading(CountDownLatch start, CountDownLatch waitToProceed, CountDownLatch stop) {
            this.start = start;
            this.stop = stop;
            this.waitToProceed = waitToProceed;
        }

        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            start.countDown();
            try {
                waitToProceed.await();
                final SimpleValue value = entry.getValue();
                value.i++;
                entry.setValue(value);
                return null;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                stop.countDown();
            }
        }

        @Override
        public EntryProcessor<String, SimpleValue, Object> getBackupProcessor() {
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

    }

    private static class EntryLatchReadOnlyVerifyingWhileOtherWriting
            implements EntryProcessor<String, SimpleValue, Object>, Offloadable, ReadOnly {

        private final CountDownLatch otherStarted;
        private final CountDownLatch otherWaitingToProceed;
        private final CountDownLatch otherStopped;

        EntryLatchReadOnlyVerifyingWhileOtherWriting(CountDownLatch otherStarted, CountDownLatch otherWaitingToProceed,
                                                     CountDownLatch otherStopped) {
            this.otherStarted = otherStarted;
            this.otherWaitingToProceed = otherWaitingToProceed;
            this.otherStopped = otherStopped;
        }

        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            if (otherStarted.getCount() != 0 || otherStopped.getCount() != 1) {
                throw new RuntimeException("Wrong threading order");
            }
            otherWaitingToProceed.countDown();
            return null;
        }

        @Override
        public EntryProcessor<String, SimpleValue, Object> getBackupProcessor() {
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }
    }

    private String init() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        IMap<Object, Object> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);
        return key;
    }

    @Test
    public void testEntryProcessorWithKey_lockedVsUnlocked() {
        String key = init();
        IMap<Object, Object> map = instances[1].getMap(MAP_NAME);

        // not locked -> will offload
        String thread = map.executeOnKey(key, new ThreadSneakingOffloadableEntryProcessor<>());
        assertTrue(thread.contains("cached.thread"));

        // locked -> won't offload
        map.lock(key);
        thread = map.executeOnKey(key, new ThreadSneakingOffloadableEntryProcessor<>());
        assertTrue(thread.contains("partition-operation.thread"));
    }

    @Test
    public void testEntryProcessorWithKey_lockedVsUnlocked_ReadOnly() {
        String key = init();
        IMap<Object, Object> map = instances[1].getMap(MAP_NAME);

        // not locked -> will offload
        String thread = map.executeOnKey(key, new ThreadSneakingOffloadableReadOnlyEntryProcessor<>());
        assertTrue(thread.contains("cached.thread"));

        // locked -> will offload
        map.lock(key);
        thread = map.executeOnKey(key, new ThreadSneakingOffloadableReadOnlyEntryProcessor<>());
        assertTrue(thread.contains("cached.thread"));
    }

    private static class ThreadSneakingOffloadableEntryProcessor<K, V>
            implements EntryProcessor<K, V, String>, Offloadable {
        @Override
        public String process(Map.Entry<K, V> entry) {
            // returns the name of thread it runs on
            return Thread.currentThread().getName();
        }

        @Override
        public String getExecutorName() {
            return OFFLOADABLE_EXECUTOR;
        }
    }

    private static class ThreadSneakingOffloadableReadOnlyEntryProcessor<K, V>
            implements EntryProcessor<K, V, String>, Offloadable, ReadOnly {
        @Override
        public String process(Map.Entry<K, V> entry) {
            // returns the name of thread it runs on
            return Thread.currentThread().getName();
        }

        @Override
        public EntryProcessor<K, V, String> getBackupProcessor() {
            return null;
        }

        @Override
        public String getExecutorName() {
            return OFFLOADABLE_EXECUTOR;
        }
    }

    @Test
    public void testEntryProcessorWithKey_localNotReentrant() throws ExecutionException, InterruptedException {
        String key = init();
        IMap<String, SimpleValue> map = instances[1].getMap(MAP_NAME);
        int count = 100;

        // when
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            futures.add(map.submitToKey(key, new IncrementingOffloadableEP()).toCompletableFuture());
        }
        FutureUtil.waitForever(futures);

        // then
        assertEquals(count + 1, map.get(key).i);
    }

    private static class IncrementingOffloadableEP
            implements EntryProcessor<String, SimpleValue, Object>, Offloadable {
        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            SimpleValue value = entry.getValue();
            value.i++;
            entry.setValue(value);
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }
    }

    @Test
    public void testEntryProcessorWithKey_localNotReentrant_latchTest() throws ExecutionException, InterruptedException {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        IMap<String, SimpleValue> map = instances[0].getMap(MAP_NAME);
        map.put(key, givenValue);

        CountDownLatch mayStart = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        CompletableFuture first = map.submitToKey(key, new EntryLatchAwaitingModifying(mayStart, stopped)).toCompletableFuture();
        mayStart.countDown();
        CompletableFuture second = map.submitToKey(key, new EntryOtherStoppedVerifying(stopped)).toCompletableFuture();

        while (!(first.isDone() && second.isDone())) {
            sleepAtLeastMillis(1);
        }

        // verifies that the other has stopped before the first one started
        assertEquals(0L, second.get());
    }

    private static class EntryLatchAwaitingModifying implements EntryProcessor<String, SimpleValue, Object>, Offloadable {

        private final CountDownLatch mayStart;
        private final CountDownLatch stop;

        EntryLatchAwaitingModifying(CountDownLatch mayStart, CountDownLatch stop) {
            this.mayStart = mayStart;
            this.stop = stop;
        }

        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            try {
                mayStart.await();
            } catch (InterruptedException e) {
            }
            try {
                entry.setValue(entry.getValue());
                return null;
            } finally {
                stop.countDown();
            }
        }

        @Override
        public EntryProcessor<String, SimpleValue, Object> getBackupProcessor() {
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

    }


    private static class EntryOtherStoppedVerifying
            implements EntryProcessor<String, SimpleValue, Long>, Offloadable {

        private final CountDownLatch otherStopped;

        EntryOtherStoppedVerifying(CountDownLatch otherStopped) {
            this.otherStopped = otherStopped;
        }

        @Override
        public Long process(final Map.Entry<String, SimpleValue> entry) {
            return otherStopped.getCount();
        }

        @Override
        public EntryProcessor<String, SimpleValue, Long> getBackupProcessor() {
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }
    }

    /**
     * <pre>
     * Given: Operation heartbeats are sent four times per {@link ClusterProperty#OPERATION_CALL_TIMEOUT_MILLIS}
     *        (see {@link InvocationMonitor#getHeartbeatBroadcastPeriodMillis()})
     * When: An offloaded EntryProcessor takes a long time to run.
     * Then: Heartbeats are still coming while the task is offloaded.
     * </pre>
     */
    @Test
    @Category(SlowTest.class)
    public void testHeartBeatsComingWhenEntryProcessorOffloaded() {
        /* Shut down the cluster since we want to use a different
         * OPERATION_CALL_TIMEOUT_MILLIS value in this test. */
        shutdownNodeFactory();

        int heartbeatsIntervalSec = 15;
        Config config = getConfig();
        config.getProperties().setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(),
                String.valueOf(heartbeatsIntervalSec * 4 * 1000));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        instances = factory.newInstances(config);

        final String key = generateKeyOwnedBy(instances[1]);
        String offloadableStartTimeRefName = "offloadableStartTimeRefName";
        String exitLatchName = "exitLatchName";
        IAtomicLong offloadableStartedTimeRef
                = instances[0].getCPSubsystem().getAtomicLong(offloadableStartTimeRefName);
        ICountDownLatch exitLatch = instances[0].getCPSubsystem().getCountDownLatch(exitLatchName);
        exitLatch.trySetCount(1);

        TimestampedSimpleValue givenValue = new TimestampedSimpleValue(1);

        final IMap<String, TimestampedSimpleValue> map = instances[0].getMap(MAP_NAME);
        map.put(key, givenValue);

        final Address instance1Address = instances[1].getCluster().getLocalMember().getAddress();
        final List<Long> heartBeatTimestamps = new LinkedList<>();
        Thread hbMonitorThread = new Thread(() -> {
            NodeEngine nodeEngine = getNodeEngineImpl(instances[0]);
            OperationServiceImpl osImpl = (OperationServiceImpl) nodeEngine.getOperationService();
            Map<Address, AtomicLong> heartBeats = osImpl.getInvocationMonitor().getHeartbeatPerMember();
            long lastbeat = Long.MIN_VALUE;
            while (!Thread.currentThread().isInterrupted()) {
                AtomicLong timestamp = heartBeats.get(instance1Address);
                if (timestamp != null) {
                    long newlastbeat = timestamp.get();
                    if (lastbeat != newlastbeat) {
                        lastbeat = newlastbeat;
                        long offloadableStartTime = offloadableStartedTimeRef.get();
                        if (offloadableStartTime != 0 && offloadableStartTime < newlastbeat) {
                            heartBeatTimestamps.add(newlastbeat);
                            exitLatch.countDown();
                        }
                    }
                }
                HazelcastTestSupport.sleepMillis(100);
            }
        });

        final int secondsToRun = 55;
        try {
            hbMonitorThread.start();
            map.executeOnKey(key, new TimeConsumingOffloadableTask(secondsToRun, offloadableStartTimeRefName, exitLatchName));
        } finally {
            hbMonitorThread.interrupt();
        }

        int heartBeatCount = 0;
        TimestampedSimpleValue updatedValue = map.get(key);
        for (long heartBeatTimestamp : heartBeatTimestamps) {
            if (heartBeatTimestamp > updatedValue.processStart
                    && heartBeatTimestamp < updatedValue.processEnd) {
                heartBeatCount++;
            }
        }

        assertTrue("Heartbeats should be received while offloadable entry processor is running. "
                + "Observed: " + heartBeatTimestamps + " EP start: " + updatedValue.processStart
                + " end: " + updatedValue.processEnd, heartBeatCount > 0);
    }

    private static class TimeConsumingOffloadableTask
            implements HazelcastInstanceAware,
                       EntryProcessor<String, TimestampedSimpleValue, Object>,
                       Offloadable,
                       Serializable {

        private final int secondsToWork;
        private final String startTimeRefName;
        private final String exitLatchName;
        private transient ICountDownLatch exitLatch;
        private transient IAtomicLong startedTime;

        private TimeConsumingOffloadableTask(int secondsToWork, String startTimeRefName, String exitLatchName) {
            this.secondsToWork = secondsToWork;
            this.startTimeRefName = startTimeRefName;
            this.exitLatchName = exitLatchName;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.exitLatch = hazelcastInstance.getCPSubsystem().getCountDownLatch(exitLatchName);
            this.startedTime = hazelcastInstance.getCPSubsystem().getAtomicLong(startTimeRefName);
        }

        @Override
        public Object process(final Map.Entry<String, TimestampedSimpleValue> entry) {
            final TimestampedSimpleValue value = entry.getValue();
            value.processStart = System.currentTimeMillis();
            startedTime.set(value.processStart);
            long endTime = TimeUnit.SECONDS.toMillis(secondsToWork) + System.currentTimeMillis() + 500L;
            do {
                HazelcastTestSupport.sleepMillis(200);
                value.i++;
                entry.setValue(value);
            } while (System.currentTimeMillis() < endTime && exitLatch.getCount() > 0);
            value.i = 1;
            entry.setValue(value);
            value.processEnd = System.currentTimeMillis();
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

    }

    private static class SimpleValue implements Serializable {

        int i;

        SimpleValue() {
        }

        SimpleValue(final int i) {
            this.i = i;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SimpleValue that = (SimpleValue) o;

            if (i != that.i) {
                return false;
            }

            return true;
        }

        @Override
        public String toString() {
            return "value: " + i;
        }
    }

    private static class TimestampedSimpleValue extends SimpleValue {
        private long processStart;
        private long processEnd;

        TimestampedSimpleValue() {
            super();
        }

        TimestampedSimpleValue(int i) {
            super(i);
        }
    }

}
