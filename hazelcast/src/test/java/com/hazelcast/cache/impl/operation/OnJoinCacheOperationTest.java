/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Test whether OnJoinCacheOperation logs warning, fails or succeeds under different JCache API availability
 * in classpath.
 */
@Category({QuickTest.class, ParallelJVMTest.class})
public class OnJoinCacheOperationTest {

    private final NodeEngine nodeEngine = mock(NodeEngine.class);
    private final ILogger logger = mock(ILogger.class);

    @Before
    public void setUp() {
        when(nodeEngine.getLogger(any(Class.class))).thenReturn(logger);
    }

    @Test
    public void test_cachePostJoinOperationSucceeds_whenJCacheAvailable_noWarningIsLogged() throws Exception {
        // node engine returns mock CacheService
        when(nodeEngine.getService(CacheService.SERVICE_NAME)).thenReturn(mock(ICacheService.class));

        // JCacheDetector finds JCache in classpath
        OnJoinCacheOperation onJoinCacheOperation = createTestOnJoinCacheOperation(true);
        onJoinCacheOperation.setNodeEngine(nodeEngine);

        onJoinCacheOperation.run();

        verify(nodeEngine).getService(CacheService.SERVICE_NAME);
        verifyNoMoreInteractions(nodeEngine);
        // verify logger was not invoked
        verify(logger, never()).warning(anyString());
    }

    @Test
    public void test_cachePostJoinOperationSucceeds_whenJCacheNotAvailable_noCacheConfigs() throws Exception {
        // JCache is not available in classpath
        OnJoinCacheOperation onJoinCacheOperation = createTestOnJoinCacheOperation(false);
        onJoinCacheOperation.setNodeEngine(nodeEngine);

        onJoinCacheOperation.run();

        verify(nodeEngine).getLogger(onJoinCacheOperation.getClass());
        // verify a warning was logged
        verify(logger).warning(anyString());
        // verify CacheService instance was not requested in OnJoinCacheOperation.run
        verifyNoMoreInteractions(nodeEngine);
    }

    @Test
    public void test_cachePostJoinOperationFails_whenJCacheAvailable_withGetServiceFailed() {
        // node engine throws HazelcastException due to missing CacheService
        when(nodeEngine.getService(CacheService.SERVICE_NAME)).thenThrow(new HazelcastException("CacheService not found"));

        // JCacheDetector finds JCache in classpath
        OnJoinCacheOperation onJoinCacheOperation = createTestOnJoinCacheOperation(true);
        onJoinCacheOperation.setNodeEngine(nodeEngine);

        assertThatThrownBy(onJoinCacheOperation::run).isInstanceOf(HazelcastException.class).hasMessage("CacheService not found");

        verify(nodeEngine).getService(CacheService.SERVICE_NAME);
        verifyNoMoreInteractions(nodeEngine);
        verify(logger, never()).warning(anyString());
    }

    @Test
    public void test_cachePostJoinOperationFails_whenJCacheNotAvailable_withCacheConfigs() {
        // JCache is not available in classpath
        OnJoinCacheOperation onJoinCacheOperation = createTestOnJoinCacheOperation(false);
        // some CacheConfigs are added in the OnJoinCacheOperation (so JCache is actually in use in the rest of the cluster)
        onJoinCacheOperation.addCacheConfig(new CacheConfig("test"));
        onJoinCacheOperation.setNodeEngine(nodeEngine);

        assertThatThrownBy(onJoinCacheOperation::run)
                .isInstanceOf(HazelcastException.class)
                .hasMessage("Service with name 'hz:impl:cacheService' not found!");

        verify(nodeEngine).getLogger(onJoinCacheOperation.getClass());
        verifyNoMoreInteractions(nodeEngine);
        verify(logger).severe(anyString());
    }

    /*
    This approach is implemented as a workaround to address the instability of mocking the static method in certain JDKs.
    It allows the test of various scenarios based on imitating the response
    of the static method JCacheDetector.isJCacheAvailable.
     */
    public OnJoinCacheOperation createTestOnJoinCacheOperation(boolean isJCacheAvailable) {
        return new OnJoinCacheOperation() {
            @Override
            public boolean isJCacheAvailable() {
                return isJCacheAvailable;
            }
        };
    }
}
