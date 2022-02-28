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

package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.nio.ByteOrder;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

import static com.hazelcast.internal.nearcache.impl.AbstractNearCachePreloaderTest.KeyType.INTEGER;
import static com.hazelcast.internal.nearcache.impl.AbstractNearCachePreloaderTest.KeyType.STRING;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.assertEqualsFormat;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.assertNearCacheInvalidationRequests;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.assertNearCacheRecord;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.assertNearCacheSize;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.assertNearCacheSizeEventually;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getNearCacheKey;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getRecordFromNearCache;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getValueFromNearCache;
import static com.hazelcast.internal.nio.IOUtil.copy;
import static com.hazelcast.internal.nio.IOUtil.deleteQuietly;
import static com.hazelcast.internal.nio.IOUtil.getFileFromResources;
import static com.hazelcast.internal.nio.IOUtil.touch;
import static com.hazelcast.test.Accessors.getSerializationService;
import static com.hazelcast.test.TimeConstants.MINUTE;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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

    protected static final int KEY_COUNT = 10023;
    protected static final int THREAD_COUNT = 10;
    protected static final int CREATE_AND_DESTROY_RUNS = 500;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected final String defaultNearCache = randomName();

    protected final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private final File preloadFile10kInt = getFileFromResources("nearcache-10k-int.store");
    private final File preloadFile10kString = getFileFromResources("nearcache-10k-string.store");
    private final File preloadFileEmpty = getFileFromResources("nearcache-empty.store");
    private final File preloadFileInvalidMagicBytes = getFileFromResources("nearcache-invalid-magicbytes.store");
    private final File preloadFileInvalidFileFormat = getFileFromResources("nearcache-invalid-fileformat.store");
    private final File preloadFileNegativeFileFormat = getFileFromResources("nearcache-negative-fileformat.store");

    @After
    public void tearDown() throws Exception {
        try {
            hazelcastFactory.shutdownAll();
        } finally {
            deleteQuietly(getStoreFile());
            deleteQuietly(getStoreLockFile());
        }
    }

    /**
     * The {@link NearCacheConfig} used by the Near Cache tests.
     * <p>
     * Needs to be set by the implementations of this class in their {@link org.junit.Before} methods.
     */
    protected NearCacheConfig nearCacheConfig;

    /**
     * Creates the {@link NearCacheTestContext} used by the Near Cache tests.
     *
     * @param <K>                     key type of the created {@link DataStructureAdapter}
     * @param <V>                     value type of the created {@link DataStructureAdapter}
     * @param createNearCacheInstance determines if a Near Cache instance should be created
     * @return a {@link NearCacheTestContext} used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createContext(boolean createNearCacheInstance);

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

    @Test(timeout = 10 * MINUTE)
    @Category(SlowTest.class)
    public void testStoreAndLoad_withIntegerKeys() {
        storeAndLoad(2342, INTEGER);
    }

    @Test(timeout = 10 * MINUTE)
    @Category(SlowTest.class)
    public void testStoreAndLoad_withStringKeys() {
        storeAndLoad(4223, STRING);
    }

    private void storeAndLoad(int keyCount, KeyType keyType) {
        nearCacheConfig.getPreloaderConfig()
                .setStoreInitialDelaySeconds(3)
                .setStoreIntervalSeconds(1);

        NearCacheTestContext<Object, String, NK, NV> context = createContext(true);

        populateDataAdapter(context, keyCount, keyType);
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

    @Test(timeout = 10 * MINUTE)
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

        createContext(true);
    }

    @Test(timeout = 10 * MINUTE)
    @Category(SlowTest.class)
    public void testCreateStoreFile_withStringKey() {
        createStoreFile(KEY_COUNT, STRING);
    }

    @Test(timeout = 10 * MINUTE)
    @Category(SlowTest.class)
    public void testCreateStoreFile_withIntegerKey() {
        createStoreFile(KEY_COUNT, INTEGER);
    }

    @Test(timeout = 10 * MINUTE)
    @Category(SlowTest.class)
    public void testCreateStoreFile_withEmptyNearCache() {
        createStoreFile(0, INTEGER);
    }

    private void createStoreFile(int keyCount, KeyType keyType) {
        nearCacheConfig.getPreloaderConfig()
                .setStoreInitialDelaySeconds(2)
                .setStoreIntervalSeconds(1);

        NearCacheTestContext<Object, String, NK, NV> context = createContext(true);

        populateDataAdapter(context, keyCount, keyType);
        populateNearCache(context, keyCount, keyType);
        waitForNearCachePersistence(context, 3);
        assertLastNearCachePersistence(context, getStoreFile(), keyCount);
    }

    @Test(timeout = 10 * MINUTE)
    @Category(SlowTest.class)
    public void testCreateStoreFile_whenTwoClientsWithSameStoreFile_thenThrowException() {
        nearCacheConfig.getPreloaderConfig()
                .setStoreInitialDelaySeconds(2)
                .setStoreIntervalSeconds(1);

        // the first client creates the lock file and holds the lock on it
        NearCacheTestContext<Object, String, NK, NV> context = createContext(true);
        populateDataAdapter(context, KEY_COUNT, INTEGER);

        // assure that the pre-loader is working on the first client
        populateNearCache(context, KEY_COUNT, INTEGER);
        waitForNearCachePersistence(context, 3);
        assertLastNearCachePersistence(context, getStoreFile(), KEY_COUNT);

        // the second client cannot acquire the lock, so it fails with an exception
        expectedException.expectMessage("Cannot acquire lock on " + getStoreFile());
        expectedException.expect(HazelcastException.class);
        createNearCacheContext();
    }

    @Test(timeout = 10 * MINUTE)
    public void testPreloadNearCache_withIntegerKeys() {
        preloadNearCache(preloadFile10kInt, 10000, INTEGER, 10 * MINUTE);
    }

    @Test(timeout = 10 * MINUTE)
    public void testPreloadNearCache_withStringKeys() {
        preloadNearCache(preloadFile10kString, 10000, STRING, 10 * MINUTE);
    }

    @Test(timeout = 10 * MINUTE)
    public void testPreloadNearCache_withEmptyFile() {
        preloadNearCache(preloadFileEmpty, 0, INTEGER, 10 * MINUTE);
    }

    @Test(timeout = 10 * MINUTE)
    public void testPreloadNearCache_withInvalidMagicBytes() {
        preloadNearCache(preloadFileInvalidMagicBytes, 0, INTEGER, 10 * MINUTE);
    }

    @Test(timeout = 10 * MINUTE)
    public void testPreloadNearCache_withInvalidFileFormat() {
        preloadNearCache(preloadFileInvalidFileFormat, 0, INTEGER, 10 * MINUTE);
    }

    @Test(timeout = 10 * MINUTE)
    public void testPreloadNearCache_withNegativeFileFormat() {
        preloadNearCache(preloadFileNegativeFileFormat, 0, INTEGER, 10 * MINUTE);
    }

    private void preloadNearCache(File preloaderFile, int keyCount, KeyType keyType, long timeoutMillis) {
        copyStoreFile(preloaderFile.getAbsoluteFile(), getStoreFile());
        NearCacheTestContext<Object, String, NK, NV> context = createContext(false);
        assumeConfiguredByteOrder(getSerializationService(context.dataInstance), ByteOrder.BIG_ENDIAN);

        // populate the member side data structure, so we have the values to populate the client side Near Cache
        populateDataAdapter(context, keyCount, keyType);

        // start the client which will kick off the Near Cache pre-loader
        NearCacheTestContext<Object, String, NK, NV> clientContext = createNearCacheContext();

        // wait until the pre-loading is done, then check for the Near Cache size
        assertNearCachePreloadDoneEventually(clientContext, MILLISECONDS.toSeconds(timeoutMillis));
        assertNearCacheSizeEventually(clientContext, keyCount);
        assertNearCacheContent(clientContext, keyCount, keyType);
    }

    @Test(timeout = 10 * MINUTE)
    public void testPreloadNearCacheLock_withSharedConfig_concurrently() {
        nearCacheConfig.getPreloaderConfig().setDirectory("");

        ThreadPoolExecutor pool = (ThreadPoolExecutor) newFixedThreadPool(THREAD_COUNT);
        final NearCacheTestContext<String, String, NK, NV> context = createContext(true);
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
                    String dataStructureName = nearCacheConfig.getName() + "-" + threadName;
                    DataStructureAdapter<String, String> adapter = getDataStructure(context, dataStructureName);
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

    @Test(timeout = 10 * MINUTE)
    public void testCreateAndDestroyDataStructure_withSameName() {
        String name = "testCreateAndDestroyDataStructure_withSameName_" + getClass().getSimpleName();
        nearCacheConfig.setName(name);

        NearCacheTestContext<Object, String, NK, NV> context = createContext(true);
        populateDataAdapter(context, KEY_COUNT, INTEGER);

        DataStructureAdapter<Object, String> adapter = context.nearCacheAdapter;
        for (int i = 0; i < CREATE_AND_DESTROY_RUNS; i++) {
            adapter.destroy();
            adapter = getDataStructure(context, name);
        }
        adapter.destroy();
    }

    @Test(timeout = 10 * MINUTE)
    public void testCreateAndDestroyDataStructure_withDifferentNames() {
        String name = "testCreateAndDestroyDataStructure_withDifferentNames_" + getClass().getSimpleName();
        nearCacheConfig.setName(name + "*");

        NearCacheTestContext<Object, String, NK, NV> context = createContext(true);
        populateDataAdapter(context, KEY_COUNT, INTEGER);

        DataStructureAdapter<Object, String> adapter = context.nearCacheAdapter;
        for (int i = 0; i < CREATE_AND_DESTROY_RUNS; i++) {
            adapter.destroy();
            adapter = getDataStructure(context, name + i);
        }
        adapter.destroy();
    }

    protected final NearCacheConfig getNearCacheConfig(InMemoryFormat inMemoryFormat, boolean serializeKeys,
                                                       boolean invalidationOnChange, int maxSize, String preloaderDir) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT)
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

    protected void populateDataAdapter(NearCacheTestContext<Object, String, ?, ?> context, int size, KeyType keyType) {
        if (size < 1) {
            return;
        }
        for (int i = 0; i < size; i++) {
            Object key = createKey(keyType, i);
            context.dataAdapter.put(key, "value-" + i);
        }
        assertNearCacheInvalidationRequests(context, size);
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

    /**
     * Copies the given Near Cache store file.
     * <p>
     * This is needed to run multiple implementations of this abstract test in parallel,
     * when using the prepared Near Cache store files. Otherwise those tests run into
     * a {@link OverlappingFileLockException}, because they use the same store file.
     *
     * @param sourceFile the source store file
     * @param targetFile the target store file
     */
    private static void copyStoreFile(File sourceFile, File targetFile) {
        // we have to touch the targetFile, otherwise copyFile creates it as directory
        touch(targetFile);
        copy(sourceFile, targetFile);
    }

    private static void waitForNearCachePersistence(final NearCacheTestContext context, final int persistenceCount) {
        final long oldPersistenceCount = context.stats.getPersistenceCount();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                long newPersistenceCount = context.stats.getPersistenceCount();
                assertTrue(format("We saw %d persistences before and were waiting for %d new persistences, but still got %d (%s)",
                        oldPersistenceCount, persistenceCount, newPersistenceCount, context.stats),
                        newPersistenceCount > oldPersistenceCount + persistenceCount);
            }
        });
    }

    private static void assertLastNearCachePersistence(NearCacheTestContext context, File defaultStoreFile, int keyCount) {
        assertEqualsFormat("Expected %d Near Cache keys to be stored, but was %d (%s)",
                keyCount, context.stats.getLastPersistenceKeyCount(), context.stats);
        if (keyCount > 0) {
            assertTrue(format("Expected the NearCache lastPersistenceWrittenBytes to be > 0 (%s)", context.stats),
                    context.stats.getLastPersistenceWrittenBytes() > 0);
            assertTrue(format("Expected the Near Cache store file %s to exist (%s)",
                    defaultStoreFile.getAbsolutePath(), context.stats),
                    defaultStoreFile.exists());
        } else {
            assertEquals(format("Expected the NearCache lastPersistenceWrittenBytes to be 0 (%s)", context.stats),
                    0, context.stats.getLastPersistenceWrittenBytes());
            assertFalse(format("Expected the Near Cache store file %s not to exist (%s)",
                    defaultStoreFile.getAbsolutePath(), context.stats),
                    defaultStoreFile.exists());
        }
        assertTrue(format("Expected no Near Cache persistence failures (%s)", context.stats),
                context.stats.getLastPersistenceFailure().isEmpty());
    }

    private static void assertNearCachePreloadDoneEventually(final NearCacheTestContext clientContext) {
        assertNearCachePreloadDoneEventually(clientContext, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    private static void assertNearCachePreloadDoneEventually(final NearCacheTestContext clientContext, long timeoutSeconds) {
        assertTrueEventually(() -> {
            NearCache nearCache = clientContext.nearCache;
            NearCacheStats stats = nearCache.getNearCacheStats();
            int size = nearCache.size();

            assertTrue(format("Preloading has not finished yet. [size: %d, %s]",
                    size, stats), nearCache.isPreloadDone());
        });
    }

    private static void assertNearCacheContent(NearCacheTestContext<?, ?, ?, ?> context, int keyCount, KeyType keyType) {
        InMemoryFormat inMemoryFormat = context.nearCacheConfig.getInMemoryFormat();
        for (int i = 0; i < keyCount; i++) {
            Object key = createKey(keyType, i);
            Object nearCacheKey = getNearCacheKey(context, key);

            String value = context.serializationService.toObject(getValueFromNearCache(context, nearCacheKey));
            assertEqualsFormat("Expected value %s in Near Cache, but found %s (%s)", "value-" + i, value, context.stats);

            assertNearCacheRecord(getRecordFromNearCache(context, nearCacheKey), i, inMemoryFormat);
        }
    }
}
