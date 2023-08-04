/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.JCacheDetector;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test whether OnJoinCacheOperation logs warning, fails or succeeds under different JCache API availability
 * in classpath.
 */
@Category({QuickTest.class, ParallelJVMTest.class})
public class OnJoinCacheOperationTest {

    private static MockedStatic<JCacheDetector> mockedStatic;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final NodeEngine nodeEngine = mock(NodeEngine.class);
    private final ILogger logger = mock(ILogger.class);

    @BeforeClass
    public static void beforeClass() throws Exception {
        mockedStatic = Mockito.mockStatic(JCacheDetector.class);
    }

    @AfterClass
    public static void cleanupMocks() {
        mockedStatic.close();
    }

    @Before
    public void setUp() {
        when(nodeEngine.getConfigClassLoader()).thenReturn(getClass().getClassLoader());
        when(nodeEngine.getLogger(any(Class.class))).thenReturn(logger);
    }

    @Test
    public void test_cachePostJoinOperationSucceeds_whenJCacheAvailable_noWarningIsLogged() throws Exception {
        // JCacheDetector finds JCache in classpath

        mockedStatic.when(() -> JCacheDetector.isJCacheAvailable(any(ClassLoader.class))).thenReturn(true);
        // node engine returns mock CacheService
        when(nodeEngine.getService(CacheService.SERVICE_NAME)).thenReturn(mock(ICacheService.class));

        OnJoinCacheOperation onJoinCacheOperation = new OnJoinCacheOperation();
        onJoinCacheOperation.setNodeEngine(nodeEngine);

        onJoinCacheOperation.run();

        verify(nodeEngine).getConfigClassLoader();
        verify(nodeEngine).getService(CacheService.SERVICE_NAME);
        // verify logger was not invoked
        verify(logger, never()).warning(anyString());

    }

    @Test
    public void test_cachePostJoinOperationSucceeds_whenJCacheNotAvailable_noCacheConfigs() throws Exception {
        mockedStatic.when(() -> JCacheDetector.isJCacheAvailable(any())).thenReturn(false);

        OnJoinCacheOperation onJoinCacheOperation = new OnJoinCacheOperation();
        onJoinCacheOperation.setNodeEngine(nodeEngine);

        onJoinCacheOperation.run();

        verify(nodeEngine).getConfigClassLoader();
        // verify a warning was logged
        verify(logger).warning(anyString());
        // verify CacheService instance was not requested in OnJoinCacheOperation.run
        verify(nodeEngine, never()).getService(CacheService.SERVICE_NAME);
    }

    @Test
    public void test_cachePostJoinOperationFails_whenJCacheNotAvailable_withCacheConfigs() throws Exception {
        // JCache is not available in classpath
        mockedStatic.when(() -> JCacheDetector.isJCacheAvailable(any())).thenReturn(false);
        // node engine throws HazelcastException due to missing CacheService
        when(nodeEngine.getService(CacheService.SERVICE_NAME)).thenThrow(new HazelcastException("CacheService not found"));

        // some CacheConfigs are added in the OnJoinCacheOperation (so JCache is actually in use in the rest of the cluster)
        OnJoinCacheOperation onJoinCacheOperation = new OnJoinCacheOperation();
        onJoinCacheOperation.addCacheConfig(new CacheConfig("test"));
        onJoinCacheOperation.setNodeEngine(nodeEngine);

        expectedException.expect(HazelcastException.class);
        onJoinCacheOperation.run();

        verify(nodeEngine).getConfigClassLoader();
        verify(nodeEngine).getService(CacheService.SERVICE_NAME);
        verify(logger, never()).warning(anyString());
    }
}
