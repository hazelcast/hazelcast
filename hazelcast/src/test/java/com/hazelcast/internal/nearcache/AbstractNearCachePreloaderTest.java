package com.hazelcast.internal.nearcache;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.nio.IOUtil.deleteQuietly;
import static com.hazelcast.nio.IOUtil.getFileFromResources;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public abstract class AbstractNearCachePreloaderTest<NK, NV> extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected static final int KEY_COUNT = 10023;
    protected static final String DEFAULT_NEAR_CACHE_NAME = "defaultNearCache";
    protected static final File DEFAULT_STORE_FILE = new File("nearCache-defaultNearCache.store").getAbsoluteFile();
    protected static final File DEFAULT_STORE_LOCK_FILE = new File(DEFAULT_STORE_FILE.getName() + ".lock").getAbsoluteFile();

    private static final int TEST_TIMEOUT = 120000;

    private final File preloadDir10kInt = getFileFromResources("nearcache-10k-int");
    private final File preloadDir10kString = getFileFromResources("nearcache-10k-string");
    private final File preloadDirEmpty = getFileFromResources("nearcache-empty");
    private final File preloadDirInvalidMagicBytes = getFileFromResources("nearcache-invalid-magicbytes");
    private final File preloadDirInvalidFileFormat = getFileFromResources("nearcache-invalid-fileformat");
    private final File preloadDirNegativeFileFormat = getFileFromResources("nearcache-negative-fileformat");

    @After
    @Before
    public void deleteFiles() {
        deleteQuietly(DEFAULT_STORE_FILE);
        deleteQuietly(DEFAULT_STORE_LOCK_FILE);
    }

    /**
     * The {@link NearCacheConfig} used by the Near Cache tests.
     *
     * Needs to be set by the implementations of this class in their {@link org.junit.Before} methods.
     */
    protected NearCacheConfig nearCacheConfig;

    /**
     * Creates the {@link NearCacheTestContext} used by the Near Cache tests.
     *
     * @param <K> key type of the created {@link com.hazelcast.internal.adapter.DataStructureAdapter}
     * @param <V> value type of the created {@link com.hazelcast.internal.adapter.DataStructureAdapter}
     * @return a {@link NearCacheTestContext} used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createContext(boolean createClient);

    /**
     * Creates the {@link NearCacheTestContext} with only a client used by the Near Cache tests.
     *
     * @param <K> key type of the created {@link com.hazelcast.internal.adapter.DataStructureAdapter}
     * @param <V> value type of the created {@link com.hazelcast.internal.adapter.DataStructureAdapter}
     * @return a {@link NearCacheTestContext} with only a client used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createClientContext();

    /**
     * Creates a storage (Map or Cache, HD or Heap) from the given context and the given name.
     *
     * @param context NearCacheTestContext used to retrieve the Member instance in order to create the storage
     * @param name The name of the storage
     * @param <K> Key type of the created {@link DataStructureAdapter}
     * @param <V> Value type of the created {@link DataStructureAdapter}
     * @return a {@link DataStructureAdapter} as created from the given context and per implementation of this class
     */
    protected abstract <K, V> DataStructureAdapter<K, V> createNewClientStore(NearCacheTestContext context, String name);

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testStoreAndLoad_withIntegerKeys() {
        storeAndLoad(2342, false);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testStoreAndLoad_withStringKeys() {
        storeAndLoad(4223, true);
    }

    private void storeAndLoad(int keyCount, boolean useStringKey) {
        nearCacheConfig.getPreloaderConfig()
                .setStoreInitialDelaySeconds(3)
                .setStoreIntervalSeconds(1);

        NearCacheTestContext<Object, String, NK, NV> context = createContext(true);

        populateNearCache(context, keyCount, useStringKey);
        waitForNearCachePersistence(context, 1);
        assertLastNearCachePersistence(context, keyCount);

        // shutdown the first client
        context.nearCacheInstance.shutdown();

        // start a new client which will kick off the Near Cache pre-loader
        NearCacheTestContext<Object, String, NK, NV> clientContext = createClientContext();

        // wait until the pre-loading is done, then check for the Near Cache size
        assertNearCachePreloadDoneEventually(clientContext);
        assertNearCacheSizeEventually(clientContext, keyCount);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testCreateStoreFile_withInvalidDirectory() {
        String directory = "/dev/null/";
        nearCacheConfig.getPreloaderConfig()
                .setStoreInitialDelaySeconds(1)
                .setStoreIntervalSeconds(1)
                .setDirectory(directory);

        expectedException.expectMessage("Cannot create lock file " + directory + DEFAULT_STORE_FILE.getName());
        expectedException.expect(HazelcastException.class);

        createContext(true);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testCreateStoreFile_withStringKey() {
        createStoreFile(KEY_COUNT, true);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testCreateStoreFile_withIntegerKey() {
        createStoreFile(KEY_COUNT, false);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testCreateStoreFile_withEmptyNearCache() {
        createStoreFile(0, false);
    }

    private void createStoreFile(int keyCount, boolean useStringKey) {
        nearCacheConfig.getPreloaderConfig()
                .setStoreInitialDelaySeconds(2)
                .setStoreIntervalSeconds(1);

        NearCacheTestContext<Object, String, NK, NV> context = createContext(true);

        populateNearCache(context, keyCount, useStringKey);
        waitForNearCachePersistence(context, 3);
        assertLastNearCachePersistence(context, keyCount);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testCreateStoreFile_whenTwoClientsWithSameStoreFile_thenThrowException() {
        nearCacheConfig.getPreloaderConfig()
                .setStoreInitialDelaySeconds(2)
                .setStoreIntervalSeconds(1);

        // the first client creates the lock file and holds the lock on it
        NearCacheTestContext<Object, String, NK, NV> context = createContext(true);

        // assure that the pre-loader is working on the first client
        populateNearCache(context, KEY_COUNT, false);
        waitForNearCachePersistence(context, 3);
        assertLastNearCachePersistence(context, KEY_COUNT);

        // the second client cannot acquire the lock, so it fails with an exception
        expectedException.expectMessage("Cannot acquire lock on " + DEFAULT_STORE_FILE);
        expectedException.expect(HazelcastException.class);
        createClientContext();
    }

    @Test
    public void testPreloadNearCache_withIntegerKeys() {
        preloadNearCache(preloadDir10kInt, 10000, false);
    }

    @Test
    public void testPreloadNearCache_withStringKeys() {
        preloadNearCache(preloadDir10kString, 10000, true);
    }

    @Test
    public void testPreloadNearCache_withEmptyFile() {
        preloadNearCache(preloadDirEmpty, 0, false);
    }

    @Test
    public void testPreloadNearCache_withInvalidMagicBytes() {
        preloadNearCache(preloadDirInvalidMagicBytes, 0, false);
    }

    @Test
    public void testPreloadNearCache_withInvalidFileFormat() {
        preloadNearCache(preloadDirInvalidFileFormat, 0, false);
    }

    @Test
    public void testPreloadNearCache_withNegativeFileFormat() {
        preloadNearCache(preloadDirNegativeFileFormat, 0, false);
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

                    DataStructureAdapter store = createNewClientStore(ctx, DEFAULT_NEAR_CACHE_NAME + Thread.currentThread());
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

    private void preloadNearCache(File preloaderDir, int keyCount, boolean useStringKey) {
        nearCacheConfig.getPreloaderConfig()
                       .setDirectory(preloaderDir.getAbsolutePath());
        NearCacheTestContext<Object, String, NK, NV> context = createContext(false);

        // populate the member side data structure, so we have the values to populate the client side Near Cache
        for (int i = 0; i < keyCount; i++) {
            Object key = useStringKey ? "key-" + i : i;
            context.dataAdapter.put(key, "value-" + i);
        }

        // start the client which will kick off the Near Cache pre-loader
        NearCacheTestContext<Object, String, NK, NV> clientContext = createClientContext();

        // wait until the pre-loading is done, then check for the Near Cache size
        assertNearCachePreloadDoneEventually(clientContext);
        assertNearCacheSizeEventually(clientContext, keyCount);
    }

    protected NearCacheConfig getNearCacheConfig(InMemoryFormat inMemoryFormat, boolean invalidationOnChange, int maxSize,
                                                 String preloaderDir) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                .setSize(maxSize)
                .setEvictionPolicy(EvictionPolicy.LRU);

        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat)
                .setInvalidateOnChange(invalidationOnChange)
                .setEvictionConfig(evictionConfig);

        nearCacheConfig.getPreloaderConfig()
                .setEnabled(true)
                .setDirectory(preloaderDir);

        return nearCacheConfig;
    }

    private void populateNearCache(NearCacheTestContext<Object, String, NK, NV> context, int keyCount, boolean useStringKey) {
        for (int i = 0; i < keyCount; i++) {
            Object key = useStringKey ? "key-" + i : i;
            context.nearCacheAdapter.put(key, "value-" + i);
            context.nearCacheAdapter.get(key);
        }
    }

    private static long getPersistenceCount(NearCacheTestContext context) {
        return context.nearCache.getNearCacheStats().getPersistenceCount();
    }

    private static void waitForNearCachePersistence(final NearCacheTestContext context, final int persistenceCount) {
        final long oldPersistenceCount = getPersistenceCount(context);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                long newPersistenceCount = getPersistenceCount(context);
                assertTrue(format("We saw %d persistences before and were waiting for %d new persistences, but still got %d",
                        oldPersistenceCount, persistenceCount, newPersistenceCount),
                        newPersistenceCount > oldPersistenceCount + persistenceCount);
            }
        });
    }

    private static void assertLastNearCachePersistence(NearCacheTestContext context, int keyCount) {
        NearCacheStats nearCacheStats = context.nearCache.getNearCacheStats();
        assertEquals(keyCount, nearCacheStats.getLastPersistenceKeyCount());
        if (keyCount > 0) {
            assertTrue(nearCacheStats.getLastPersistenceWrittenBytes() > 0);
            assertTrue(DEFAULT_STORE_FILE.exists());
        } else {
            assertEquals(0, nearCacheStats.getLastPersistenceWrittenBytes());
            assertFalse(DEFAULT_STORE_FILE.exists());
        }
        assertTrue(nearCacheStats.getLastPersistenceFailure().isEmpty());
    }

    private static void assertNearCachePreloadDoneEventually(final NearCacheTestContext clientContext) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(clientContext.nearCache.isPreloadDone());
            }
        });
    }

    private static void assertNearCacheSizeEventually(final NearCacheTestContext context, final int expectedSize) {
        // assert the Near Cache size
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                NearCacheStats nearCacheStats = context.nearCache.getNearCacheStats();
                assertEquals("Expected to have " + expectedSize + " entries in the Near Cache " + nearCacheStats,
                        expectedSize, context.nearCache.size());
                assertEquals("Expected to have " + expectedSize + " entries in the Near Cache " + nearCacheStats,
                        expectedSize, nearCacheStats.getOwnedEntryCount());
            }
        });
    }
}
