package com.hazelcast.client.cache.nearcache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.adapter.ICacheDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractBasicNearCacheTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
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

import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;

/**
 * Basic Near Cache tests for {@link ICache} on Hazelcast clients.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class BasicClientCacheNearCacheTest extends AbstractBasicNearCacheTest<Data, String> {

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public LocalUpdatePolicy localUpdatePolicy;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Parameters(name = "format:{0} {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.BINARY, LocalUpdatePolicy.CACHE_ON_UPDATE},
                {InMemoryFormat.OBJECT, LocalUpdatePolicy.INVALIDATE},
                {InMemoryFormat.OBJECT, LocalUpdatePolicy.CACHE_ON_UPDATE},
        });
    }

    @Before
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(inMemoryFormat)
                .setLocalUpdatePolicy(localUpdatePolicy);
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Override
    protected <K, V> com.hazelcast.internal.nearcache.NearCacheTestContext<K, V, Data, String> createContext() {
        ClientConfig clientConfig = getClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        CacheConfig<K, V> cacheConfig = createCacheConfig(nearCacheConfig);

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(getConfig());
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);

        CachingProvider memberProvider = HazelcastServerCachingProvider.createCachingProvider(member);
        HazelcastServerCacheManager memberCacheManager = (HazelcastServerCacheManager) memberProvider.getCacheManager();

        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();
        String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(DEFAULT_NEAR_CACHE_NAME);

        ICache<K, V> clientCache = cacheManager.createCache(DEFAULT_NEAR_CACHE_NAME, cacheConfig);
        ICache<K, V> memberCache = memberCacheManager.createCache(DEFAULT_NEAR_CACHE_NAME, cacheConfig);

        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(cacheNameWithPrefix);

        return new NearCacheTestContext<K, V, Data, String>(
                client.getSerializationService(),
                client,
                member,
                new ICacheDataStructureAdapter<K, V>(clientCache),
                new ICacheDataStructureAdapter<K, V>(memberCache),
                false,
                nearCache,
                nearCacheManager,
                cacheManager,
                memberCacheManager
        );
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private <K, V> CacheConfig<K, V> createCacheConfig(NearCacheConfig nearCacheConfig) {
        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>()
                .setName(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(nearCacheConfig.getInMemoryFormat());

        if (nearCacheConfig.getInMemoryFormat() == NATIVE) {
            cacheConfig.getEvictionConfig()
                    .setEvictionPolicy(LRU)
                    .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                    .setSize(90);
        }

        return cacheConfig;
    }

    @Test
    @Override
    @Ignore(value = "The AbstractClientCacheProxy is missing `invalidateNearCache(keyData)` calls")
    public void whenPutAllIsUsed_thenNearCacheShouldBeInvalidated_withUpdateOnNearCacheAdapter() {
    }
}
