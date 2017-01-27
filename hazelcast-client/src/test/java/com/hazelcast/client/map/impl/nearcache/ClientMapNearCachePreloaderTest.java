package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.adapter.IMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractNearCachePreloaderTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ClientMapNearCachePreloaderTest extends AbstractNearCachePreloaderTest<Data, String> {

    private static final File DEFAULT_STORE_FILE = new File("nearCache-defaultNearCache.store").getAbsoluteFile();
    private static final File DEFAULT_STORE_LOCK_FILE = new File(DEFAULT_STORE_FILE.getName() + ".lock").getAbsoluteFile();

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public boolean invalidationOnChange;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Parameters(name = "format:{0} invalidationOnChange:{1}")
    public static Collection<Object[]> parameters() {
        // FIXME: the Near Cache pre-loader doesn't work with enabled invalidations due to a known getAll() issue!
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY, false},
                //{InMemoryFormat.BINARY, true},
                {InMemoryFormat.OBJECT, false},
                //{InMemoryFormat.OBJECT, true},
        });
    }

    @Before
    public void setUp() {
        nearCacheConfig = getNearCacheConfig(inMemoryFormat, invalidationOnChange, KEY_COUNT, DEFAULT_STORE_FILE.getParent());
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void testPreloadNearCacheLock_withSharedMapConfig_concurrently()
            throws InterruptedException {

        // Ignore other memory formats, this test is not affected by that option.
        assumeTrue(BINARY.equals(nearCacheConfig.getInMemoryFormat()));

        nearCacheConfig.getPreloaderConfig().setDirectory("");

        final NearCacheTestContext ctx = createContext(true);

        int nThreads = 10;
        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(nThreads);
        final CountDownLatch startLatch = new CountDownLatch(nThreads);
        final CountDownLatch finishLatch = new CountDownLatch(nThreads);

        for (int i = 0; i < nThreads; i++) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    startLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    IMap<String, String> store = ctx.nearCacheInstance.getMap(DEFAULT_NEAR_CACHE_NAME + Thread.currentThread());
                    for (int i = 0; i < 100; i++) {
                        store.put("Test_" + Thread.currentThread() + "_" + i, "Value_" + Thread.currentThread() + "_" + i);
                    }

                    finishLatch.countDown();
                }
            });
        }

        finishLatch.await();
        pool.shutdownNow();
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
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(boolean createClient) {
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(getConfig());
        IMap<K, V> memberMap = member.getMap(DEFAULT_NEAR_CACHE_NAME);

        if (!createClient) {
            return new NearCacheTestContext<K, V, Data, String>(
                    getSerializationService(member),
                    member,
                    new IMapDataStructureAdapter<K, V>(memberMap),
                    false,
                    null,
                    null);
        }

        NearCacheTestContext<K, V, Data, String> clientContext = createClientContext();
        return new NearCacheTestContext<K, V, Data, String>(
                clientContext.serializationService,
                clientContext.nearCacheInstance,
                member,
                clientContext.nearCacheAdapter,
                new IMapDataStructureAdapter<K, V>(memberMap),
                false,
                clientContext.nearCache,
                clientContext.nearCacheManager);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createClientContext() {
        ClientConfig clientConfig = getClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        IMap<K, V> clientMap = client.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        return new NearCacheTestContext<K, V, Data, String>(
                client.getSerializationService(),
                client,
                null,
                new IMapDataStructureAdapter<K, V>(clientMap),
                null,
                false,
                nearCache,
                nearCacheManager);
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }
}
