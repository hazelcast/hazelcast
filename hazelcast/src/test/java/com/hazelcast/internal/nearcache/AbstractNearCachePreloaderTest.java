package com.hazelcast.internal.nearcache;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;

import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.nio.IOUtil.deleteQuietly;
import static com.hazelcast.nio.IOUtil.getFileFromResources;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class AbstractNearCachePreloaderTest<NK, NV> extends HazelcastTestSupport {

    protected static final int KEY_COUNT = 10023;
    protected static final String DEFAULT_NEAR_CACHE_NAME = "defaultNearCache";
    protected static final File DEFAULT_STORE_FILE = new File("nearCache-defaultNearCache.store").getAbsoluteFile();

    private static final int TEST_TIMEOUT = 120000;

    private final File preloadFile10kInt = getFileFromResources("nearcache-10k-int.store");
    private final File preloadFile10kString = getFileFromResources("nearcache-10k-string.store");
    private final File preloadFileEmpty = getFileFromResources("nearcache-empty.store");
    private final File preloadFileInvalidMagicBytes = getFileFromResources("nearcache-invalid-magicbytes.store");
    private final File preloadFileInvalidFileFormat = getFileFromResources("nearcache-invalid-fileformat.store");
    private final File preloadFileNegativeFileFormat = getFileFromResources("nearcache-negative-fileformat.store");

    @BeforeClass
    public static void initLogger() {
        setLoggingLog4j();
        setLogLevel(Level.INFO);
    }

    @AfterClass
    public static void resetLogger() {
        resetLogLevel();
    }

    @After
    public void deleteFiles() {
        deleteQuietly(DEFAULT_STORE_FILE);
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
     * @param <K> key type of the created {@link DataStructureAdapter}
     * @param <V> value type of the created {@link DataStructureAdapter}
     * @return a {@link NearCacheTestContext} used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createContext();

    /**
     * Creates the {@link NearCacheTestContext} with only a client used by the Near Cache tests.
     *
     * @param <K> key type of the created {@link DataStructureAdapter}
     * @param <V> value type of the created {@link DataStructureAdapter}
     * @return a {@link NearCacheTestContext} with only a client used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createClientContext();

    @Test(timeout = TEST_TIMEOUT)
    public void testStoreAndLoad_withIntegerKeys() {
        storeAndLoad(2342, false);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void testStoreAndLoad_withStringKeys() {
        storeAndLoad(4223, true);
    }

    private void storeAndLoad(int keyCount, boolean useStringKey) {
        nearCacheConfig.getPreloaderConfig()
                .setStoreInitialDelaySeconds(3)
                .setStoreIntervalSeconds(1);

        NearCacheTestContext<Object, String, NK, NV> context = createContext();

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
    public void testCreateStoreFile_withInvalidFileName() {
        nearCacheConfig.getPreloaderConfig()
                .setStoreInitialDelaySeconds(1)
                .setStoreIntervalSeconds(1)
                .setFileName("/dev/null/invalid.store");

        NearCacheTestContext<Object, String, NK, NV> context = createContext();
        populateNearCache(context, KEY_COUNT, false);

        // there are no Near Cache statistics for failing persistences, so we just wait a bit
        sleepSeconds(3);
        assertLastNearCachePersistence(context, 0);
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
                .setStoreInitialDelaySeconds(3)
                .setStoreIntervalSeconds(1);

        NearCacheTestContext<Object, String, NK, NV> context = createContext();

        populateNearCache(context, keyCount, useStringKey);
        waitForNearCachePersistence(context, 3);
        assertLastNearCachePersistence(context, keyCount);
    }

    @Test
    public void testPreloadNearCache_withIntegerKeys() {
        preloadNearCache(preloadFile10kInt, 10000, false);
    }

    @Test
    public void testPreloadNearCache_withStringKeys() {
        preloadNearCache(preloadFile10kString, 10000, true);
    }

    @Test
    public void testPreloadNearCache_withEmptyFile() {
        preloadNearCache(preloadFileEmpty, 0, false);
    }

    @Test
    public void testPreloadNearCache_withInvalidMagicBytes() {
        preloadNearCache(preloadFileInvalidMagicBytes, 0, false);
    }

    @Test
    public void testPreloadNearCache_withInvalidFileFormat() {
        preloadNearCache(preloadFileInvalidFileFormat, 0, false);
    }

    @Test
    public void testPreloadNearCache_withNegativeFileFormat() {
        preloadNearCache(preloadFileNegativeFileFormat, 0, false);
    }

    private void preloadNearCache(File preloaderFile, int keyCount, boolean useStringKey) {
        nearCacheConfig.getPreloaderConfig().setFileName(preloaderFile.getAbsolutePath());
        NearCacheTestContext<Object, String, NK, NV> context = createContext();

        // populate the member side cache, so we have the values to populate the Near Cache
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

    protected NearCacheConfig getNearCacheConfig(InMemoryFormat inMemoryFormat, int size, String preloaderFileName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                .setSize(size)
                .setEvictionPolicy(EvictionPolicy.LRU);

        NearCacheConfig nearCacheConfig = createNearCacheConfig(inMemoryFormat)
                .setInvalidateOnChange(false)
                .setEvictionConfig(evictionConfig);

        nearCacheConfig.getPreloaderConfig()
                .setEnabled(true)
                .setFileName(preloaderFileName);

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
