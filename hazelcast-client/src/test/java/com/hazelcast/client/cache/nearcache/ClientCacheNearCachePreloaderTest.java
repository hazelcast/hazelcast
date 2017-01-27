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
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.adapter.ICacheDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractNearCachePreloaderTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.cache.spi.CachingProvider;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.cache.CacheUtil.getDistributedObjectName;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.nio.IOUtil.toFileName;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ClientCacheNearCachePreloaderTest extends AbstractNearCachePreloaderTest<Data, String> {

    private static final String CACHE_FILE_NAME = toFileName(getDistributedObjectName(DEFAULT_NEAR_CACHE_NAME));
    private static final File DEFAULT_STORE_FILE = new File("nearCache-" + CACHE_FILE_NAME + ".store").getAbsoluteFile();
    private static final File DEFAULT_STORE_LOCK_FILE = new File(DEFAULT_STORE_FILE.getName() + ".lock").getAbsoluteFile();

    @Parameter
    public boolean invalidationOnChange;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Parameters(name = "invalidationOnChange:{0}")
    public static Collection<Object[]> parameters() {
        // FIXME: the Near Cache pre-loader doesn't work with enabled invalidations due to a known getAll() issue!
        return Arrays.asList(new Object[][]{
                {false},
                //{true},
        });
    }

    @Before
    public void setUp() {
        nearCacheConfig = getNearCacheConfig(invalidationOnChange, KEY_COUNT, DEFAULT_STORE_FILE.getParent());
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Override
    protected File getDefaultStoreFile() {
        return DEFAULT_STORE_FILE;
    }

    @Override
    protected File getDefaultStoreLockFile() {
        return DEFAULT_STORE_LOCK_FILE;
    }

    @Override
    protected <K, V> com.hazelcast.internal.nearcache.NearCacheTestContext<K, V, Data, String> createContext(
            boolean createClient) {
        CacheConfig<K, V> cacheConfig = createCacheConfig(nearCacheConfig);

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(getConfig());
        CachingProvider memberProvider = HazelcastServerCachingProvider.createCachingProvider(member);
        HazelcastServerCacheManager memberCacheManager = (HazelcastServerCacheManager) memberProvider.getCacheManager();
        ICache<K, V> memberCache = memberCacheManager.createCache(DEFAULT_NEAR_CACHE_NAME, cacheConfig);

        if (!createClient) {
            return new NearCacheTestContext<K, V, Data, String>(
                    getSerializationService(member),
                    member,
                    new ICacheDataStructureAdapter<K, V>(memberCache),
                    false,
                    null,
                    null);
        }

        NearCacheTestContext<K, V, Data, String> clientContext = createClientContext(cacheConfig);
        return new com.hazelcast.internal.nearcache.NearCacheTestContext<K, V, Data, String>(
                clientContext.serializationService,
                clientContext.nearCacheInstance,
                member,
                clientContext.nearCacheAdapter,
                new ICacheDataStructureAdapter<K, V>(memberCache),
                false,
                clientContext.nearCache,
                clientContext.nearCacheManager,
                clientContext.cacheManager,
                memberCacheManager);
    }

    @Override
    protected <K, V> com.hazelcast.internal.nearcache.NearCacheTestContext<K, V, Data, String> createClientContext() {
        CacheConfig<K, V> cacheConfig = createCacheConfig(nearCacheConfig);
        return createClientContext(cacheConfig);
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

    private <K, V> NearCacheTestContext<K, V, Data, String> createClientContext(CacheConfig<K, V> cacheConfig) {
        ClientConfig clientConfig = getClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);

        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();
        String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(DEFAULT_NEAR_CACHE_NAME);

        ICache<K, V> clientCache = cacheManager.createCache(DEFAULT_NEAR_CACHE_NAME, cacheConfig);

        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(cacheNameWithPrefix);

        return new com.hazelcast.internal.nearcache.NearCacheTestContext<K, V, Data, String>(
                client.getSerializationService(),
                client,
                null,
                new ICacheDataStructureAdapter<K, V>(clientCache),
                null,
                false,
                nearCache,
                nearCacheManager,
                cacheManager,
                null);
    }
}
