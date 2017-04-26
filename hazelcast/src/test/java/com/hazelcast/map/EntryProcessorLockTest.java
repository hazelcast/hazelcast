package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Offloadable;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.LockAwareLazyMapEntry;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
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
public class EntryProcessorLockTest extends HazelcastTestSupport {

    public static final String MAP_NAME = "EntryProcessorLockTest";

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {BINARY}, {OBJECT}
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
        Map<String, Object> result = map.executeOnEntries(new TestNonOffloadableEntryProcessor(), TruePredicate.INSTANCE);

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

        Boolean result = (Boolean) map.submitToKey("key1", new TestNonOffloadableEntryProcessor()).get();

        assertFalse(result);
    }

    @Test
    public void test_submitToKey_Offloadable() throws ExecutionException, InterruptedException {
        IMap<String, String> map = getInitializedMap();

        Boolean result = (Boolean) map.submitToKey("key1", new TestOffloadableEntryProcessor()).get();

        assertNull(result);
    }

    @Test
    public void test_submitToKey_Offloadable_ReadOnly() throws ExecutionException, InterruptedException {
        IMap<String, String> map = getInitializedMap();

        Boolean result = (Boolean) map.submitToKey("key1", new TestOffloadableReadOnlyEntryProcessor()).get();

        assertNull(result);
    }

    @Test
    public void test_Serialization_LockAwareLazyMapEntry_deserializesAs_LazyMapEntry() throws ExecutionException, InterruptedException {
        InternalSerializationService ss = getSerializationService(createHazelcastInstance(getConfig()));
        LockAwareLazyMapEntry entry = new LockAwareLazyMapEntry(ss.toData("key"), "value", ss, Extractors.empty(), false);

        LockAwareLazyMapEntry deserializedEntry = ss.toObject(ss.toData(entry));

        assertEquals(LockAwareLazyMapEntry.class, deserializedEntry.getClass());
        assertEquals("key", deserializedEntry.getKey());
        assertEquals("value", deserializedEntry.getValue());
        assertEquals(null, deserializedEntry.isLocked());
    }

    private static class TestNonOffloadableEntryProcessor implements EntryProcessor {
        @Override
        public Object process(Map.Entry entry) {
            return ((LockAware) entry).isLocked();
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
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
        public EntryBackupProcessor getBackupProcessor() {
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
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }
    }

}
