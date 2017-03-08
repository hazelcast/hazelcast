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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.JCacheDetector;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test whether PostJoinCacheOperation logs warning, fails or succeeds under different JCache API availability
 * in classpath.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JCacheDetector.class)
@Category({QuickTest.class, ParallelTest.class})
public class PostJoinCacheOperationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private NodeEngine nodeEngine = mock(NodeEngine.class);
    private ClassLoader classLoader = mock(ClassLoader.class);
    private ILogger logger = mock(ILogger.class);

    @Before
    public void setUp() {
        PowerMockito.mockStatic(JCacheDetector.class);
        when(nodeEngine.getConfigClassLoader()).thenReturn(classLoader);
        when(nodeEngine.getLogger(any(Class.class))).thenReturn(logger);
    }

    @Test
    public void test_cachePostJoinOperationSucceeds_whenJCacheAvailable_noWarningIsLogged() throws Exception {
        // JCacheDetector finds JCache in classpath
        when(JCacheDetector.isJCacheAvailable(classLoader)).thenReturn(true);
        // node engine returns mock CacheService
        when(nodeEngine.getService(CacheService.SERVICE_NAME)).thenReturn(mock(ICacheService.class));

        PostJoinCacheOperation postJoinCacheOperation = new PostJoinCacheOperation();
        postJoinCacheOperation.setNodeEngine(nodeEngine);

        postJoinCacheOperation.run();

        verify(nodeEngine).getConfigClassLoader();
        verify(nodeEngine).getService(CacheService.SERVICE_NAME);
        // verify logger was not invoked
        verify(logger, never()).warning(anyString());
    }

    @Test
    public void test_cachePostJoinOperationSucceeds_whenJCacheNotAvailable_noCacheConfigs() throws Exception {
        when(JCacheDetector.isJCacheAvailable(classLoader)).thenReturn(false);

        PostJoinCacheOperation postJoinCacheOperation = new PostJoinCacheOperation();
        postJoinCacheOperation.setNodeEngine(nodeEngine);

        postJoinCacheOperation.run();

        verify(nodeEngine).getConfigClassLoader();
        // verify a warning was logged
        verify(logger).warning(anyString());
        // verify CacheService instance was not requested in PostJoinCacheOperation.run
        verify(nodeEngine, never()).getService(CacheService.SERVICE_NAME);
    }

    @Test
    public void test_cachePostJoinOperationFails_whenJCacheNotAvailable_withCacheConfigs() throws Exception {
        // JCache is not available in classpath
        when(JCacheDetector.isJCacheAvailable(classLoader)).thenReturn(false);
        // node engine throws HazelcastException due to missing CacheService
        when(nodeEngine.getService(CacheService.SERVICE_NAME)).thenThrow(new HazelcastException("CacheService not found"));

        // some CacheConfigs are added in the PostJoinCacheOperation (so JCache is actually in use in the rest of the cluster)
        PostJoinCacheOperation postJoinCacheOperation = new PostJoinCacheOperation();
        postJoinCacheOperation.addCacheConfig(new CacheConfig("test"));
        postJoinCacheOperation.setNodeEngine(nodeEngine);

        expectedException.expect(HazelcastException.class);
        postJoinCacheOperation.run();

        verify(nodeEngine).getConfigClassLoader();
        verify(nodeEngine).getService(CacheService.SERVICE_NAME);
        verify(logger, never()).warning(anyString());
    }
}
