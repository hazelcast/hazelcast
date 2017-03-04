package com.hazelcast.map.impl.tx;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.adapter.TransactionalMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractBasicNearCacheTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheStats;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getMapNearCacheManager;

/**
 * Basic Near Cache tests for {@link com.hazelcast.core.TransactionalMap} on Hazelcast members.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class BasicTxnMapNearCacheTest extends AbstractBasicNearCacheTest<Data, String> {

    @Parameter
    public InMemoryFormat inMemoryFormat;

    private final TestHazelcastInstanceFactory hazelcastFactory = createHazelcastInstanceFactory(2);

    @Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT},
        });
    }

    @Before
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(inMemoryFormat)
                .setCacheLocalEntries(true)
                // we have to configure invalidation, otherwise the Near Cache in the TransactionalMap will not be used
                .setInvalidateOnChange(true);
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext() {
        Config config = getConfig();
        config.getMapConfig(DEFAULT_NEAR_CACHE_NAME).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance[] instances = hazelcastFactory.newInstances(config);
        HazelcastInstance member = instances[0];

        // this creates the Near Cache instance
        member.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = getMapNearCacheManager(member);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContext<K, V, Data, String>(
                getSerializationService(member),
                member,
                new TransactionalMapDataStructureAdapter<K, V>(member, DEFAULT_NEAR_CACHE_NAME),
                false,
                nearCache,
                nearCacheManager);
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the TransactionalMap due to its limited implementation")
    public void whenCacheIsFull_thenPutOnSameKeyShouldUpdateValue_withUpdateOnNearCacheAdapter() {
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the TransactionalMap due to its limited implementation")
    public void whenCacheIsFull_thenPutOnSameKeyShouldUpdateValue_withUpdateOnDataAdapter() {
    }

    @Test
    @Override
    public void testNearCacheStats() {
        // we cannot use the common test, since the Near Cache support in TransactionalMap is very limited
        NearCacheTestContext<Integer, String, Data, String> context = createContext();

        IMap<Integer, String> map = context.nearCacheInstance.getMap(DEFAULT_NEAR_CACHE_NAME);

        // populate map
        populateMap(context);
        assertNearCacheStats(context, 0, 0, 0);

        // uses txMap.get() which reads from the Near Cache, but doesn't populate it (so we just create misses)
        populateNearCache(context);
        assertNearCacheStats(context, 0, 0, DEFAULT_RECORD_COUNT);

        // use map.get() which populates the Near Cache (but also increases the misses first)
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            map.get(i);
        }
        assertNearCacheStats(context, DEFAULT_RECORD_COUNT, 0, DEFAULT_RECORD_COUNT * 2);

        // make some hits
        populateNearCache(context);
        assertNearCacheStats(context, DEFAULT_RECORD_COUNT, DEFAULT_RECORD_COUNT, DEFAULT_RECORD_COUNT * 2);
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the TransactionalMap due to its limited implementation")
    public void testNearCacheMemoryCostCalculation_withConcurrentCacheMisses() {
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the TransactionalMap due to its limited implementation")
    public void testGetAsyncPopulatesNearCache() throws Exception {
    }

    @Test
    @Override
    @Ignore(value = "This test doesn't work with the TransactionalMap due to its limited implementation")
    public void testNearCacheEviction() {
    }
}
