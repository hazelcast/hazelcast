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

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Offloadable;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.LockAwareLazyMapEntry;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.test.Accessors.getSerializationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryProcessorLockTest extends HazelcastTestSupport {

    public static final String MAP_NAME = "EntryProcessorLockTest";

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameters(name = "{index}: {0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {BINARY},
                {OBJECT},
        });
    }

    @Override
    public Config getConfig() {
        Config config = super.getConfig();
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setInMemoryFormat(inMemoryFormat);
        config.addMapConfig(mapConfig);
        return config;
    }

    private IMap<String, String> getInitializedMap() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, String> map = instance.getMap(MAP_NAME);
        map.put("key1", "value1");
        map.put("key2", "value2");
        return map;
    }

    @Test
    public void test_executeOnEntries() {
        IMap<String, String> map = getInitializedMap();

        map.lock("key1");
        Map<String, Object> result = map.executeOnEntries(new TestNonOffloadableEntryProcessor());

        assertTrue((Boolean) result.get("key1"));
        assertFalse((Boolean) result.get("key2"));
    }

    @Test
    public void test_executeOnEntries_withPredicate() {
        IMap<String, String> map = getInitializedMap();

        map.lock("key1");
        Map<String, Object> result = map.executeOnEntries(new TestNonOffloadableEntryProcessor(), Predicates.alwaysTrue());

        assertTrue((Boolean) result.get("key1"));
        assertFalse((Boolean) result.get("key2"));
    }

    @Test
    public void test_executeOnKeys() {
        IMap<String, String> map = getInitializedMap();

        map.lock("key1");
        Map<String, Object> result = map.executeOnKeys(new HashSet<String>(asList("key1", "key2")),
                new TestNonOffloadableEntryProcessor());

        assertTrue((Boolean) result.get("key1"));
        assertFalse((Boolean) result.get("key2"));
    }

    @Test
    public void test_executeOnKey_notOffloadable() {
        IMap<String, String> map = getInitializedMap();

        Boolean result = (Boolean) map.executeOnKey("key1", new TestNonOffloadableEntryProcessor());

        assertFalse(result);
    }

    @Test
    public void test_executeOnKey_Offloadable() {
        IMap<String, String> map = getInitializedMap();

        Boolean result = (Boolean) map.executeOnKey("key1", new TestOffloadableEntryProcessor());

        assertNull(result);
    }

    @Test
    public void test_executeOnKey_Offloadable_ReadOnly() {
        IMap<String, String> map = getInitializedMap();

        Boolean result = (Boolean) map.executeOnKey("key1", new TestOffloadableReadOnlyEntryProcessor());

        assertNull(result);
    }

    @Test
    public void test_submitToKey_notOffloadable() throws ExecutionException, InterruptedException {
        IMap<String, String> map = getInitializedMap();

        Boolean result = (Boolean) map.submitToKey("key1", new TestNonOffloadableEntryProcessor()).toCompletableFuture().get();

        assertFalse(result);
    }

    @Test
    public void test_submitToKey_Offloadable() throws ExecutionException, InterruptedException {
        IMap<String, String> map = getInitializedMap();

        Boolean result = (Boolean) map.submitToKey("key1", new TestOffloadableEntryProcessor()).toCompletableFuture().get();

        assertNull(result);
    }

    @Test
    public void test_submitToKey_Offloadable_ReadOnly() throws ExecutionException, InterruptedException {
        IMap<String, String> map = getInitializedMap();

        Boolean result = (Boolean) map.submitToKey("key1", new TestOffloadableReadOnlyEntryProcessor())
                                      .toCompletableFuture().get();

        assertNull(result);
    }

    @Test
    public void test_Serialization_LockAwareLazyMapEntry_deserializesAs_LazyMapEntry() throws ExecutionException, InterruptedException {
        InternalSerializationService ss = getSerializationService(createHazelcastInstance(getConfig()));
        LockAwareLazyMapEntry entry = new LockAwareLazyMapEntry(ss.toData("key"), "value", ss,
                Extractors.newBuilder(ss).build(), false);

        LockAwareLazyMapEntry deserializedEntry = ss.toObject(ss.toData(entry));

        assertEquals(LockAwareLazyMapEntry.class, deserializedEntry.getClass());
        assertEquals("key", deserializedEntry.getKey());
        assertEquals("value", deserializedEntry.getValue());
        assertNull(deserializedEntry.isLocked());
    }

    private static class TestNonOffloadableEntryProcessor implements EntryProcessor {
        @Override
        public Object process(Map.Entry entry) {
            return ((LockAware) entry).isLocked();
        }

        @Override
        public EntryProcessor getBackupProcessor() {
            return null;
        }
    }

    private static class TestOffloadableEntryProcessor implements EntryProcessor, Offloadable {
        @Override
        public String getExecutorName() {
            return OFFLOADABLE_EXECUTOR;
        }

        @Override
        public Object process(Map.Entry entry) {
            return ((LockAware) entry).isLocked();
        }

        @Override
        public EntryProcessor getBackupProcessor() {
            return null;
        }
    }

    private static class TestOffloadableReadOnlyEntryProcessor implements EntryProcessor, Offloadable, ReadOnly {
        @Override
        public String getExecutorName() {
            return OFFLOADABLE_EXECUTOR;
        }

        @Override
        public Object process(Map.Entry entry) {
            return ((LockAware) entry).isLocked();
        }

        @Override
        public EntryProcessor getBackupProcessor() {
            return null;
        }
    }

}
