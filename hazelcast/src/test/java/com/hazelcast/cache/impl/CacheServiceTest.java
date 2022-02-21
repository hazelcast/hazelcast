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

package com.hazelcast.cache.impl;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheServiceTest {

    private static final String CACHE_MANAGER_PREFIX = "/test/";
    private static final String CACHE_NAME = "test-cache";
    private static final String PREFIXED_CACHE_NAME = CACHE_MANAGER_PREFIX + CACHE_NAME;
    private static final int CONCURRENCY = Runtime.getRuntime().availableProcessors();

    private NodeEngine mockNodeEngine;
    private CountDownLatch latch;
    private ExecutorService executorService;

    @Before
    public void setup() {
        // setup mocks
        ExecutionService executionService = mock(ExecutionService.class);
        ManagedExecutorService executorService = mock(ManagedExecutorService.class);
        when(executionService.getExecutor(anyString())).thenReturn(executorService);
        mockNodeEngine = Mockito.mock(NodeEngine.class);
        when(mockNodeEngine.getLogger(any(Class.class))).thenReturn(Logger.getLogger(CacheServiceTest.class));
        when(mockNodeEngine.getExecutionService()).thenReturn(executionService);

        latch = new CountDownLatch(1);
    }

    @After
    public void tearDown() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Test
    public void testPutCacheConfigConcurrently()
            throws ExecutionException, InterruptedException {
        CacheService cacheService = new TestCacheService(mockNodeEngine, false);

        executorService = Executors.newFixedThreadPool(CONCURRENCY);
        List<Future<CacheConfig>> futures = new ArrayList<Future<CacheConfig>>();
        for (int i = 0; i < CONCURRENCY; i++) {
            futures.add(
                executorService.submit(new PutCacheConfigRunnable(cacheService, latch))
            );
        }
        latch.countDown();
        FutureUtil.waitWithDeadline(futures, 10, TimeUnit.SECONDS);

        int nullCacheConfigs = 0;
        assertEquals(1, cacheService.getCacheConfigs().size());
        CacheConfig expectedCacheConfig = cacheService.getCacheConfig(PREFIXED_CACHE_NAME);
        for (int i = 0; i < CONCURRENCY; i++) {
            CacheConfig actualCacheConfig = futures.get(i).get();
            if (actualCacheConfig == null) {
                nullCacheConfigs++;
            } else {
                assertSame(expectedCacheConfig, actualCacheConfig);
            }
        }
        // we only expect 1 execution of putCacheConfigIfAbsent to put a new CacheConfigFuture in the configs map
        assertEquals(1, nullCacheConfigs);
    }

    @Test
    public void testPutCacheConfigConcurrently_whenExceptionThrownFromAdditionalSetup() {
        CacheService cacheService = new TestCacheService(mockNodeEngine, true);

        executorService = Executors.newFixedThreadPool(CONCURRENCY);
        List<Future<CacheConfig>> futures = new ArrayList<Future<CacheConfig>>();
        for (int i = 0; i < CONCURRENCY; i++) {
            futures.add(
                executorService.submit(new PutCacheConfigRunnable(cacheService, latch))
            );
        }
        latch.countDown();
        final AtomicInteger exceptionCounter = new AtomicInteger();
        FutureUtil.waitWithDeadline(futures, 10, TimeUnit.SECONDS, new FutureUtil.ExceptionHandler() {
            @Override
            public void handleException(Throwable throwable) {
                exceptionCounter.getAndIncrement();
            }
        });

        assertNull(cacheService.getCacheConfig(PREFIXED_CACHE_NAME));
        // ensure all executions completed exceptionally
        assertEquals(CONCURRENCY, exceptionCounter.get());
    }

    @Test
    public void testPutCacheConfig_whenExceptionThrownFromAdditionalSetup() {
        CacheService cacheService = new TestCacheService(mockNodeEngine, true);

        try {
            cacheService.putCacheConfigIfAbsent(newCacheConfig());
            // ensure the exception was not swallowed
            fail("InvalidConfigurationException should have been thrown");
        } catch (InvalidConfigurationException e) {
            // assert the CacheConfigFuture was not put in the configs map
            assertNull(cacheService.getCacheConfig(PREFIXED_CACHE_NAME));
        }
    }

    public static class PutCacheConfigRunnable implements Callable<CacheConfig> {
        private final CacheService cacheService;
        private final CountDownLatch latch;

        PutCacheConfigRunnable(CacheService cacheService, CountDownLatch latch) {
            this.cacheService = cacheService;
            this.latch = latch;
        }

        @Override
        public CacheConfig call()
                throws InterruptedException {
            latch.await();
            return cacheService.putCacheConfigIfAbsent(newCacheConfig());
        }
    }

    public static class TestCacheService extends CacheService {
        private final boolean throwsException;

        TestCacheService(NodeEngine nodeEngine, boolean throwsException) {
            super();
            this.nodeEngine = nodeEngine;
            this.logger = nodeEngine.getLogger(TestCacheService.class);
            this.throwsException = throwsException;
        }

        @Override
        protected void additionalCacheConfigSetup(CacheConfig config, boolean existingConfig) {
            if (throwsException) {
                throw new InvalidConfigurationException("fail");
            }
        }
    }

    private static CacheConfig newCacheConfig() {
        return new CacheConfig().setName(CACHE_NAME)
                                .setManagerPrefix(CACHE_MANAGER_PREFIX)
                                .setBackupCount(3);
    }
}
