/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Offloadable;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class EntryProcessorOffloadableTest extends HazelcastTestSupport {

    public static final String MAP_NAME = "EntryProcessorOffloadableTest";

    private HazelcastInstance[] instances;

    @Parameterized.Parameter(0)
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public int syncBackupCount;

    @Parameterized.Parameter(2)
    public int asyncBackupCount;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Parameterized.Parameters(name = "{index}: {0} sync={1} async={2}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {BINARY, 0, 0}, {OBJECT, 0, 0},
                {BINARY, 1, 0}, {OBJECT, 1, 0},
                {BINARY, 0, 1}, {OBJECT, 0, 1}
        });
    }

    private boolean isBackup() {
        return syncBackupCount + asyncBackupCount > 0;
    }

    @Override
    public Config getConfig() {
        Config config = super.getConfig();
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

        IMap<Object, Object> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);
        Object result = map.executeOnKey(key, new EntryIncOffloadable());

        assertEquals(expectedValue, map.get(key));
        assertBackupEventually(instances[1], MAP_NAME, key, isBackup() ? expectedValue : null);
        assertEquals(givenValue.i, result);


        instances[0].shutdown();
        assertEquals(expectedValue, map.get(key));
        assertEquals(givenValue.i, result);
    }

    private static class EntryIncOffloadable implements EntryProcessor<String, SimpleValue>, Offloadable, EntryBackupProcessor<String, SimpleValue> {
        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            final SimpleValue value = entry.getValue();
            int result = value.i;
            value.i++;
            entry.setValue(value);
            return result;
        }

        @Override
        public EntryBackupProcessor<String, SimpleValue> getBackupProcessor() {
            return this;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
        }
    }

    @Test
    public void testEntryProcessorWithKey_offloadable_withoutSetValue() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        SimpleValue expectedValue = new SimpleValue(1);

        IMap<Object, Object> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);
        Object result = map.executeOnKey(key, new EntryIncOffloadableNoSetValue());

        assertEquals(expectedValue, map.get(key));
        if (inMemoryFormat.equals(OBJECT)) {
            assertBackupEventually(instances[1], MAP_NAME, key, isBackup() ? new SimpleValue(2) : null);
        } else {
            assertBackupEventually(instances[1], MAP_NAME, key, isBackup() ? expectedValue : null);
        }
        assertEquals(givenValue.i, result);

        instances[0].shutdown();
        assertEquals(expectedValue, map.get(key));
    }

    private static class EntryIncOffloadableNoSetValue implements EntryProcessor<String, SimpleValue>, Offloadable, EntryBackupProcessor<String, SimpleValue> {
        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
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
        public EntryBackupProcessor getBackupProcessor() {
            return this;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
        }
    }

    @Test
    public void testEntryProcessorWithKey_offloadableReadOnly_setValue() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);

        IMap<Object, Object> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);

        expectedException.expect(UnsupportedOperationException.class);

        map.executeOnKey(key, new EntryIncOffloadableReadOnly());
    }

    private static class EntryIncOffloadableReadOnly implements EntryProcessor<String, SimpleValue>, Offloadable, ReadOnly,
            EntryBackupProcessor<String, SimpleValue> {
        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            final SimpleValue value = entry.getValue();
            int result = value.i;
            value.i++;
            entry.setValue(value);
            return result;
        }

        @Override
        public EntryBackupProcessor<String, SimpleValue> getBackupProcessor() {
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
        }
    }


    @Test
    public void testEntryProcessorWithKey_offloadableReadOnly_withoutSetValue() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        SimpleValue expectedValue = new SimpleValue(1);

        IMap<Object, Object> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);
        Object result = map.executeOnKey(key, new EntryIncOffloadableReadOnlyNoSetValue());

        assertEquals(expectedValue, map.get(key));
        assertBackupEventually(instances[1], MAP_NAME, key, isBackup() ? expectedValue : null);
        assertEquals(givenValue.i, result);

        instances[0].shutdown();
        assertEquals(expectedValue, map.get(key));
    }

    private static class EntryIncOffloadableReadOnlyNoSetValue implements EntryProcessor<String, SimpleValue>, Offloadable, ReadOnly,
            EntryBackupProcessor<String, SimpleValue> {
        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
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
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
        }
    }

    @Test
    public void testEntryProcessorWithKey_offloadableReadOnly_returnValue() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        SimpleValue expectedValue = new SimpleValue(1);

        IMap<Object, Object> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);
        Object result = map.executeOnKey(key, new EntryIncOffloadableReadOnlyReturnValue());

        assertEquals(expectedValue, map.get(key));
        assertBackupEventually(instances[1], MAP_NAME, key, isBackup() ? expectedValue : null);
        assertEquals(givenValue.i, result);

        instances[0].shutdown();
        assertEquals(expectedValue, map.get(key));
    }

    private static class EntryIncOffloadableReadOnlyReturnValue implements EntryProcessor<String, SimpleValue>, Offloadable, ReadOnly,
            EntryBackupProcessor<String, SimpleValue> {
        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            final SimpleValue value = entry.getValue();
            return value.i;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
        }
    }


    @Test
    public void testEntryProcessorWithKey_offloadableReadOnly_noReturnValue() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        SimpleValue expectedValue = new SimpleValue(1);

        IMap<Object, Object> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);
        Object result = map.executeOnKey(key, new EntryIncOffloadableReadOnlyNoReturnValue());
        assertEquals(expectedValue, map.get(key));
        assertBackupEventually(instances[1], MAP_NAME, key, isBackup() ? expectedValue : null);
        assertNull(result);

        instances[0].shutdown();
        assertEquals(expectedValue, map.get(key));
    }

    private static class EntryIncOffloadableReadOnlyNoReturnValue implements EntryProcessor<String, SimpleValue>, Offloadable, ReadOnly,
            EntryBackupProcessor<String, SimpleValue> {
        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
        }
    }

    @Test
    public void testEntryProcessorWithKey_offloadableReadOnly_throwsException() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);

        IMap<Object, Object> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);

        expectedException.expect(RuntimeException.class);
        map.executeOnKey(key, new EntryIncOffloadableReadOnlyException());
        assertFalse(map.isLocked(key));
    }

    private static class EntryIncOffloadableReadOnlyException implements EntryProcessor<String, SimpleValue>, Offloadable, ReadOnly,
            EntryBackupProcessor<String, SimpleValue> {
        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            throw new RuntimeException("EP exception");
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
        }
    }


    @Test
    public void testEntryProcessorWithKey_offloadableModifying_throwsException_keyNotLocked() {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);

        IMap<Object, Object> map = instances[1].getMap(MAP_NAME);
        map.put(key, givenValue);

        expectedException.expect(RuntimeException.class);
        map.executeOnKey(key, new EntryIncOffloadableException());

        assertFalse(map.isLocked(key));
    }

    private static class EntryIncOffloadableException implements EntryProcessor<String, SimpleValue>, Offloadable,
            EntryBackupProcessor<String, SimpleValue> {
        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            throw new RuntimeException("EP exception");
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
        }
    }

    void assertBackupEventually(final HazelcastInstance instance, final String mapName, final Object key, Object expected) {
        assertEqualsEventually(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return readFromMapBackup(instance, mapName, key);
            }
        }, expected);
    }

    @Test
    public void testEntryProcessorWithKey_offloadable_otherModifyingWillWait() throws InterruptedException {
        final String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        SimpleValue expectedValue = new SimpleValue(4);

        final IMap<Object, Object> map = instances[0].getMap(MAP_NAME);
        map.put(key, givenValue);

        final CountDownLatch epStarted = new CountDownLatch(1);
        final CountDownLatch epStopped = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.executeOnKey(key, new EntryLatchModifying(epStarted, epStopped));
            }
        }.start();

        epStarted.await();
        map.executeOnKey(key, new EntryLatchVerifying(epStarted, epStopped, 4));

        // verified EPs not out-of-order, and not at the same time
        assertEqualsEventually(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return map.get(key);
            }
        }, expectedValue);
    }

    private static class EntryLatchModifying implements EntryProcessor<String, SimpleValue>, Offloadable, EntryBackupProcessor<String, SimpleValue> {

        private final CountDownLatch start;
        private final CountDownLatch stop;

        public EntryLatchModifying(CountDownLatch start, CountDownLatch stop) {
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
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

    }

    private static class EntryLatchVerifying implements EntryProcessor<String, SimpleValue>, Offloadable, EntryBackupProcessor<String, SimpleValue> {

        private final CountDownLatch otherStarted;
        private final CountDownLatch otherStopped;
        private final int valueToSet;

        public EntryLatchVerifying(CountDownLatch otherStarted, CountDownLatch otherStopped, final int value) {
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
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
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

        final IMap<Object, Object> map = instances[0].getMap(MAP_NAME);
        map.put(key, givenValue);

        final CountDownLatch epStarted = new CountDownLatch(1);
        final CountDownLatch epWaitToProceed = new CountDownLatch(1);
        final CountDownLatch epStopped = new CountDownLatch(1);
        new Thread() {
            public void run() {
                map.executeOnKey(key, new EntryLatchModifyingOtherReading(epStarted, epWaitToProceed, epStopped));
            }
        }.start();

        epStarted.await();
        map.executeOnKey(key, new EntryLatchReadOnlyVerifyingWhileOtherWriting(epStarted, epWaitToProceed, epStopped));
        epStopped.await();

        // verified EPs not out-of-order, and not at the same time
        assertEqualsEventually(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return map.get(key);
            }
        }, expectedValue);
    }

    private static class EntryLatchModifyingOtherReading implements EntryProcessor<String, SimpleValue>, Offloadable, EntryBackupProcessor<String, SimpleValue> {

        private final CountDownLatch start;
        private final CountDownLatch stop;
        private final CountDownLatch waitToProceed;

        public EntryLatchModifyingOtherReading(CountDownLatch start, CountDownLatch waitToProceed, CountDownLatch stop) {
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
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

    }

    private static class EntryLatchReadOnlyVerifyingWhileOtherWriting implements EntryProcessor<String, SimpleValue>,
            EntryBackupProcessor<String, SimpleValue>, Offloadable, ReadOnly {

        private final CountDownLatch otherStarted;
        private final CountDownLatch otherWaitingToProceed;
        private final CountDownLatch otherStopped;

        public EntryLatchReadOnlyVerifyingWhileOtherWriting(CountDownLatch otherStarted, CountDownLatch otherWaitingToProceed,
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
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
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
        String thread = (String) map.executeOnKey(key, new ThreadSneakingOffloadableEntryProcessor());
        assertTrue(thread.contains("cached.thread"));

        // locked -> won't offload
        map.lock(key);
        thread = (String) map.executeOnKey(key, new ThreadSneakingOffloadableEntryProcessor());
        assertTrue(thread.contains("partition-operation.thread"));
    }

    @Test
    public void testEntryProcessorWithKey_lockedVsUnlocked_ReadOnly() {
        String key = init();
        IMap<Object, Object> map = instances[1].getMap(MAP_NAME);

        // not locked -> will offload
        String thread = (String) map.executeOnKey(key, new ThreadSneakingOffloadableReadOnlyEntryProcessor());
        assertTrue(thread.contains("cached.thread"));

        // locked -> will offload
        map.lock(key);
        thread = (String) map.executeOnKey(key, new ThreadSneakingOffloadableReadOnlyEntryProcessor());
        assertTrue(thread.contains("cached.thread"));
    }

    private static class ThreadSneakingOffloadableEntryProcessor extends AbstractEntryProcessor<String, SimpleValue>
            implements Offloadable {
        @Override
        public Object process(Map.Entry<String, SimpleValue> entry) {
            // returns the name of thread it runs on
            return Thread.currentThread().getName();
        }

        @Override
        public String getExecutorName() {
            return OFFLOADABLE_EXECUTOR;
        }
    }

    private static class ThreadSneakingOffloadableReadOnlyEntryProcessor implements EntryProcessor<String, SimpleValue>,
            Offloadable, ReadOnly {
        @Override
        public Object process(Map.Entry<String, SimpleValue> entry) {
            // returns the name of thread it runs on
            return Thread.currentThread().getName();
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
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
        IMap<Object, SimpleValue> map = instances[1].getMap(MAP_NAME);
        int count = 100;

        // when
        List<ICompletableFuture> futures = new ArrayList<ICompletableFuture>();
        for (int i = 0; i < count; i++) {
            futures.add(map.submitToKey(key, new IncrementingOffloadableEP()));
        }
        for (ICompletableFuture future : futures) {
            future.get();
        }

        // then
        assertEquals(count + 1, map.get(key).i);
    }

    private static class IncrementingOffloadableEP implements EntryProcessor<String, SimpleValue>, Offloadable,
            EntryBackupProcessor<String, SimpleValue> {
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

        @Override
        public EntryBackupProcessor getBackupProcessor() {
            return this;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
        }
    }

    @Test
    public void testEntryProcessorWithKey_localNotReentrant_latchTest() throws ExecutionException, InterruptedException {
        String key = generateKeyOwnedBy(instances[0]);
        SimpleValue givenValue = new SimpleValue(1);
        IMap<Object, Object> map = instances[0].getMap(MAP_NAME);
        map.put(key, givenValue);

        CountDownLatch mayStart = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        ICompletableFuture first = map.submitToKey(key, new EntryLatchAwaitingModifying(mayStart, stopped));
        mayStart.countDown();
        ICompletableFuture second = map.submitToKey(key, new EntryOtherStoppedVerifying(stopped));

        while (!(first.isDone() && second.isDone())) {
            sleepAtLeastMillis(1);
        }

        // verifies that the other has stopped before the first one started
        assertEquals(0L, second.get());
    }

    private static class EntryLatchAwaitingModifying implements EntryProcessor<String, SimpleValue>, Offloadable, EntryBackupProcessor<String, SimpleValue> {

        private final CountDownLatch mayStart;
        private final CountDownLatch stop;

        public EntryLatchAwaitingModifying(CountDownLatch mayStart, CountDownLatch stop) {
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
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public void processBackup(Map.Entry<String, SimpleValue> entry) {
            process(entry);
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }

    }


    private static class EntryOtherStoppedVerifying implements EntryProcessor<String, SimpleValue>, Offloadable {

        private final CountDownLatch otherStopped;

        public EntryOtherStoppedVerifying(CountDownLatch otherStopped) {
            this.otherStopped = otherStopped;
        }

        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            return otherStopped.getCount();
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public String getExecutorName() {
            return Offloadable.OFFLOADABLE_EXECUTOR;
        }
    }


    private static class SimpleValue implements Serializable {

        public int i;

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


}
