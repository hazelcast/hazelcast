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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

import static com.hazelcast.internal.nearcache.AbstractNearCachePreloaderTest.KeyType.INTEGER;
import static com.hazelcast.internal.nearcache.AbstractNearCachePreloaderTest.KeyType.STRING;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheInvalidations;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheRecord;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheSize;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.assertNearCacheSizeEventually;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getNearCacheKey;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getRecordFromNearCache;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getValueFromNearCache;
import static com.hazelcast.nio.IOUtil.deleteQuietly;
import static com.hazelcast.nio.IOUtil.getFileFromResources;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Contains the logic code for unified Near Cache preloader tests.
 *
 * @param <NK> key type of the tested Near Cache
 * @param <NV> value type of the tested Near Cache
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractNearCachePreloaderTest<NK, NV> extends HazelcastTestSupport {

    /**
     * The type of the key of the used {@link com.hazelcast.internal.adapter.DataStructureAdapter}.
     */
    public enum KeyType {
        INTEGER,
        STRING,
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected static final int TEST_TIMEOUT = 10 * 60 * 1000;
    protected static final int KEY_COUNT = 10023;
    protected static final int THREAD_COUNT = 10;
    protected static final int CREATE_AND_DESTROY_RUNS = 5000;

    protected final String defaultNearCache = randomName();

    private final File preloadDir10kInt = getFileFromResources("nearcache-10k-int");
    private final File preloadDir10kString = getFileFromResources("nearcache-10k-string");
    private final File preloadDirEmpty = getFileFromResources("nearcache-empty");
    private final File preloadDirInvalidMagicBytes = getFileFromResources("nearcache-invalid-magicbytes");
    private final File preloadDirInvalidFileFormat = getFileFromResources("nearcache-invalid-fileformat");
    private final File preloadDirNegativeFileFormat = getFileFromResources("nearcache-negative-fileformat");

    @After
    public void deleteFiles() {
        deleteQuietly(getStoreFile());
        deleteQuietly(getStoreLockFile());
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
     * @param <K>                     key type of the created {@link DataStructureAdapter}
     * @param <V>                     value type of the created {@link DataStructureAdapter}
     * @param createNearCacheInstance determines if a Near Cache instance should be created
     * @param keyCount                determines the size the backing {@link DataStructureAdapter} should be populated with
     * @param keyType                 determines the {@link KeyType} of the created keys
     * @return a {@link NearCacheTestContext} used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createContext(boolean createNearCacheInstance, int keyCount,
                                                                               KeyType keyType);

    /**
     * Creates the {@link NearCacheTestContext} with only a Near Cache instance used by the Near Cache tests.
     *
     * @param <K> key type of the created {@link com.hazelcast.internal.adapter.DataStructureAdapter}
     * @param <V> value type of the created {@link com.hazelcast.internal.adapter.DataStructureAdapter}
     * @return a {@link NearCacheTestContext} with only a client used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createNearCacheContext();

    /**
     * Returns the default store file used for the current implementation (IMap or JCache)
     *
     * @return the default store file
     */
    protected abstract File getStoreFile();

    /**
     * Returns the default store lock file used for the current implementation (IMap or JCache)
     *
     * @return the default store lock file
     */
    protected abstract File getStoreLockFile();

    /**
     * Returns a new {@link DataStructureAdapter} from the Near Cache side of the given {@link NearCacheTestContext}.
     *
     * @param context the {@link NearCacheTestContext} to retrieve the data structure from
     * @param name    the name for the new data structure
     * @return a new {@link DataStructureAdapter} with the given name
     */
    protected abstract <K, V> DataStructureAdapter<K, V> getDataStructure(NearCacheTestContext<K, V, NK, NV> context,
                                                                          String name);

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testStoreAndLoad_withIntegerKeys() {
        storeAndLoad(2342, INTEGER);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testStoreAndLoad_withStringKeys() {
        storeAndLoad(4223, STRING);
    }

    private void storeAndLoad(int keyCount, KeyType keyType) {
        nearCacheConfig.getPreloaderConfig()
                .setStoreInitialDelaySeconds(3)
                .setStoreIntervalSeconds(1);

        NearCacheTestContext<Object, String, NK, NV> context = createContext(true, keyCount, keyType);

        populateNearCache(context, keyCount, keyType);
        waitForNearCachePersistence(context, 1);
        assertLastNearCachePersistence(context, getStoreFile(), keyCount);

        // shutdown the first client
        context.nearCacheInstance.shutdown();

        // start a new client which will kick off the Near Cache pre-loader
        NearCacheTestContext<Object, String, NK, NV> clientContext = createNearCacheContext();

        // wait until the pre-loading is done, then check for the Near Cache size
        assertNearCachePreloadDoneEventually(clientContext);
        assertNearCacheSizeEventually(clientContext, keyCount);
        assertNearCacheContent(clientContext, keyCount, keyType);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testCreateStoreFile_withInvalidDirectory() {
        String directory = "/dev/null/";
        nearCacheConfig.getPreloaderConfig()
                .setStoreInitialDelaySeconds(1)
                .setStoreIntervalSeconds(1)
                .setDirectory(directory);

        File lockFile = new File(directory, getStoreFile().getName());
        expectedException.expectMessage("Cannot create lock file " + lockFile.getAbsolutePath());
        expectedException.expect(HazelcastException.class);

        createContext(true, 0, null);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testCreateStoreFile_withStringKey() {
        createStoreFile(KEY_COUNT, STRING);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testCreateStoreFile_withIntegerKey() {
        createStoreFile(KEY_COUNT, INTEGER);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testCreateStoreFile_withEmptyNearCache() {
        createStoreFile(0, INTEGER);
    }

    private void createStoreFile(int keyCount, KeyType keyType) {
        nearCacheConfig.getPreloaderConfig()
                .setStoreInitialDelaySeconds(2)
                .setStoreIntervalSeconds(1);

        NearCacheTestContext<Object, String, NK, NV> context = createContext(true, keyCount, keyType);

        populateNearCache(context, keyCount, keyType);
        waitForNearCachePersistence(context, 3);
        assertLastNearCachePersistence(context, getStoreFile(), keyCount);
    }

    @Test(timeout = TEST_TIMEOUT)
    @Category(SlowTest.class)
    public void testCreateStoreFile_whenTwoClientsWithSameStoreFile_thenThrowException() {
        nearCacheConfig.getPreloaderConfig()
                .setStoreInitialDelaySeconds(2)
                .setStoreIntervalSeconds(1);

        // the first client creates the lock file and holds the lock on it
        NearCacheTestContext<Object, String, NK, NV> context = createContext(true, KEY_COUNT, INTEGER);

        // assure that the pre-loader is working on the first client
        populateNearCache(context, KEY_COUNT, INTEGER);
        waitForNearCachePersistence(context, 3);
        assertLastNearCachePersistence(context, getStoreFile(), KEY_COUNT);

        // the second client cannot acquire the lock, so it fails with an exception
        expectedException.expectMessage("Cannot acquire lock on " + getStoreFile());
        expectedException.expect(HazelcastException.class);
        createNearCacheContext();
    }

    @Test
    public void testPreloadNearCache_withIntegerKeys() {
        preloadNearCache(preloadDir10kInt, 10000, INTEGER);
    }

    @Test
    public void testPreloadNearCache_withStringKeys() {
        preloadNearCache(preloadDir10kString, 10000, STRING);
    }

    @Test
    public void testPreloadNearCache_withEmptyFile() {
        preloadNearCache(preloadDirEmpty, 0, INTEGER);
    }

    @Test
    public void testPreloadNearCache_withInvalidMagicBytes() {
        preloadNearCache(preloadDirInvalidMagicBytes, 0, INTEGER);
    }

    @Test
    public void testPreloadNearCache_withInvalidFileFormat() {
        preloadNearCache(preloadDirInvalidFileFormat, 0, INTEGER);
    }

    @Test
    public void testPreloadNearCache_withNegativeFileFormat() {
        preloadNearCache(preloadDirNegativeFileFormat, 0, INTEGER);
    }

    private void preloadNearCache(File preloaderDir, int keyCount, KeyType keyType) {
        nearCacheConfig
                .setName("defaultNearCache")
                .getPreloaderConfig().setDirectory(preloaderDir.getAbsolutePath());
        NearCacheTestContext<Object, String, NK, NV> context = createContext(false, keyCount, keyType);

        // populate the member side data structure, so we have the values to populate the client side Near Cache
        for (int i = 0; i < keyCount; i++) {
            Object key = createKey(keyType, i);
            context.dataAdapter.put(key, "value-" + i);
        }
        assertNearCacheInvalidations(context, keyCount);

        // start the client which will kick off the Near Cache pre-loader
        NearCacheTestContext<Object, String, NK, NV> clientContext = createNearCacheContext();

        // wait until the pre-loading is done, then check for the Near Cache size
        assertNearCachePreloadDoneEventually(clientContext);
        assertNearCacheSizeEventually(clientContext, keyCount);
        assertNearCacheContent(clientContext, keyCount, keyType);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void testPreloadNearCacheLock_withSharedConfig_concurrently() {
        nearCacheConfig.getPreloaderConfig().setDirectory("");

        ThreadPoolExecutor pool = (ThreadPoolExecutor) newFixedThreadPool(THREAD_COUNT);
        final NearCacheTestContext<String, String, NK, NV> context = createContext(true, 0, null);
        final CountDownLatch startLatch = new CountDownLatch(THREAD_COUNT);
        final CountDownLatch finishLatch = new CountDownLatch(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    startLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        currentThread().interrupt();
                    }

                    String threadName = currentThread().toString();
                    DataStructureAdapter<String, String> adapter = getDataStructure(context,
                            nearCacheConfig.getName() + "-" + threadName);
                    for (int i = 0; i < 100; i++) {
                        adapter.put("key-" + threadName + "-" + i, "value-" + threadName + "-" + i);
                    }

                    finishLatch.countDown();
                }
            });
        }

        assertOpenEventually(finishLatch);
        pool.shutdownNow();
    }

    @Test(timeout = TEST_TIMEOUT)
    public void testCreateAndDestroyDataStructure_withSameName() {
        nearCacheConfig.setName("createDestroyNearCache");

        NearCacheTestContext<Object, String, NK, NV> context = createContext(true, KEY_COUNT, INTEGER);
        DataStructureAdapter<Object, String> adapter = context.nearCacheAdapter;

        for (int i = 0; i < CREATE_AND_DESTROY_RUNS; i++) {
            adapter.destroy();
            adapter = getDataStructure(context, "createDestroyNearCache");
        }
    }

    @Test(timeout = TEST_TIMEOUT)
    public void testCreateAndDestroyDataStructure_withDifferentNames() {
        nearCacheConfig.setName("createDestroyNearCache-*");

        NearCacheTestContext<Object, String, NK, NV> context = createContext(true, KEY_COUNT, INTEGER);
        DataStructureAdapter<Object, String> adapter = context.nearCacheAdapter;

        for (int i = 0; i < CREATE_AND_DESTROY_RUNS; i++) {
            adapter.destroy();
            adapter = getDataStructure(context, "createDestroyNearCache-" + i);
        }
    }

    protected final NearCacheConfig getNearCacheConfig(InMemoryFormat inMemoryFormat, boolean serializeKeys,
                                                       boolean invalidationOnChange, int maxSize, String preloaderDir) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                .setSize(maxSize)
                .setEvictionPolicy(EvictionPolicy.LRU);

        NearCacheConfig nearCacheConfig = createNearCacheConfig(InMemoryFormat.BINARY, true)
                .setName(defaultNearCache)
                .setInMemoryFormat(inMemoryFormat)
                .setSerializeKeys(serializeKeys)
                .setInvalidateOnChange(invalidationOnChange)
                .setEvictionConfig(evictionConfig);

        nearCacheConfig.getPreloaderConfig()
                .setEnabled(true)
                .setDirectory(preloaderDir);

        return nearCacheConfig;
    }

    @SuppressWarnings("unchecked")
    protected static void populateDataAdapter(DataStructureAdapter<?, ?> dataAdapter, int size, KeyType keyType) {
        if (size < 1) {
            return;
        }
        DataStructureAdapter<Object, String> adapter = (DataStructureAdapter<Object, String>) dataAdapter;
        for (int i = 0; i < size; i++) {
            Object key = createKey(keyType, i);
            adapter.put(key, "value-" + i);
        }
    }

    private static void populateNearCache(NearCacheTestContext<Object, String, ?, ?> context, int keyCount, KeyType keyType) {
        for (int i = 0; i < keyCount; i++) {
            Object key = createKey(keyType, i);
            context.nearCacheAdapter.get(key);
        }
        assertNearCacheSize(context, keyCount);
    }

    private static Object createKey(KeyType keyType, int i) {
        switch (keyType) {
            case STRING:
                return "key-" + i;
            case INTEGER:
                return i;
            default:
                throw new IllegalArgumentException("Unknown keyType: " + keyType);
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

    private static void assertLastNearCachePersistence(NearCacheTestContext context, File defaultStoreFile, int keyCount) {
        NearCacheStats nearCacheStats = context.nearCache.getNearCacheStats();
        assertEquals(keyCount, nearCacheStats.getLastPersistenceKeyCount());
        if (keyCount > 0) {
            assertTrue(nearCacheStats.getLastPersistenceWrittenBytes() > 0);
            assertTrue(defaultStoreFile.exists());
        } else {
            assertEquals(0, nearCacheStats.getLastPersistenceWrittenBytes());
            assertFalse(defaultStoreFile.exists());
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

    private static void assertNearCacheContent(NearCacheTestContext<?, ?, ?, ?> context, int keyCount, KeyType keyType) {
        InMemoryFormat inMemoryFormat = context.nearCacheConfig.getInMemoryFormat();
        for (int i = 0; i < keyCount; i++) {
            Object key = createKey(keyType, i);
            Object nearCacheKey = getNearCacheKey(context, key);

            String value = context.serializationService.toObject(getValueFromNearCache(context, nearCacheKey));
            assertEquals("value-" + i, value);

            assertNearCacheRecord(getRecordFromNearCache(context, nearCacheKey), i, inMemoryFormat);
        }
    }
}
