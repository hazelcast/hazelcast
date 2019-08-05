/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheGetCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheConfiguration;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.spi.CachingProvider;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CallbackAwareClientDelegatingFutureTest extends HazelcastTestSupport {

    private static final String CACHE_NAME = "MyCache";
    private TestHazelcastFactory factory;
    private HazelcastClientInstanceImpl client;

    @Before
    public void setup() {
        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance();
        client = ((HazelcastClientProxy) factory.newHazelcastClient()).client;
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void test_CallbackAwareClientDelegatingFuture_when_noTimeOut_noError() throws ExecutionException, InterruptedException {
        test_CallbackAwareClientDelegatingFuture(false, false);
    }

    @Test
    public void test_CallbackAwareClientDelegatingFuture_when_timeOut_but_noError() throws ExecutionException, InterruptedException {
        test_CallbackAwareClientDelegatingFuture(true, false);
    }

    @Test
    public void test_CallbackAwareClientDelegatingFuture_when_noTimeOut_but_error() throws ExecutionException, InterruptedException {
        test_CallbackAwareClientDelegatingFuture(false, true);
    }

    private void test_CallbackAwareClientDelegatingFuture(boolean timeout, boolean error)
            throws ExecutionException, InterruptedException {
        if (timeout && error) {
            throw new IllegalArgumentException(
                    "Only one of the `timeout` and `error` parameters can be enabled at the same time!");
        }

        int timeoutMillis = timeout ? 5000 : -1;

        createCache(timeoutMillis, error);

        ClientMessage getRequest = createGetRequest(1);
        ClientInvocation invocation = new ClientInvocation(client, getRequest, null, 0);
        ClientInvocationFuture invocationFuture = invocation.invoke();

        final AtomicBoolean responseCalled = new AtomicBoolean();
        final AtomicBoolean failureCalled = new AtomicBoolean();
        OneShotExecutionCallback callback = new OneShotExecutionCallback() {
            @Override
            protected void onResponseInternal(Object response) {
                responseCalled.set(true);
            }

            @Override
            protected void onFailureInternal(Throwable t) {
                failureCalled.set(true);
            }
        };

        CallbackAwareClientDelegatingFuture callbackAwareInvocationFuture =
                new CallbackAwareClientDelegatingFuture(invocationFuture,
                        client.getSerializationService(),
                        clientMessage -> CacheGetCodec.decodeResponse(clientMessage).response,
                        callback);

        if (timeoutMillis > 0) {
            try {
                callbackAwareInvocationFuture.get(timeoutMillis / 2, TimeUnit.MILLISECONDS);
                fail("Timeout expected!");
            } catch (TimeoutException e) {
                // Timeout expected
                assertTrue(failureCalled.get());
                assertFalse(responseCalled.get());
            }
        } else {
            if (error) {
                try {
                    callbackAwareInvocationFuture.get();
                    fail("CacheLoaderException expected!");
                } catch (ExecutionException e) {
                    // Exception expected
                    assertTrue(e.getCause() instanceof CacheLoaderException);
                    assertTrue(failureCalled.get());
                    assertFalse(responseCalled.get());
                }
            } else {
                try {
                    callbackAwareInvocationFuture.get();
                    assertTrue(responseCalled.get());
                    assertFalse(failureCalled.get());
                } catch (CacheLoaderException e) {
                    fail("CacheLoaderException not expected!");
                }
            }
        }
    }

    private Cache createCache(int blockMilis, boolean throwError) {
        CachingProvider cachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheConfiguration cacheConfig =
                new CacheConfig()
                        .setReadThrough(true)
                        .setCacheLoaderFactory(new BlockableCacheLoaderFactory(blockMilis, throwError));
        return cacheManager.createCache(CACHE_NAME, cacheConfig);
    }

    private ClientMessage createGetRequest(Integer key) {
        SerializationService serializationService = client.getSerializationService();
        Data keyData = serializationService.toData(key);
        Data expiryPolicyData = serializationService.toData(null);
        String cacheNameWithPrefix = HazelcastCacheManager.CACHE_MANAGER_PREFIX + CACHE_NAME;
        return CacheGetCodec.encodeRequest(cacheNameWithPrefix, keyData, expiryPolicyData);
    }

    public static class BlockableCacheLoaderFactory implements Factory<BlockableCacheLoader> {

        private final int blockMillis;
        private final boolean throwError;

        public BlockableCacheLoaderFactory(int blockMillis, boolean throwError) {
            this.blockMillis = blockMillis;
            this.throwError = throwError;
        }

        @Override
        public BlockableCacheLoader create() {
            return new BlockableCacheLoader(blockMillis, throwError);
        }

    }

    public static class BlockableCacheLoader implements CacheLoader<Integer, String> {

        private final int blockMillis;
        private final boolean throwError;

        public BlockableCacheLoader(int blockMillis, boolean throwError) {
            this.blockMillis = blockMillis;
            this.throwError = throwError;
        }

        @Override
        public String load(Integer key) throws CacheLoaderException {
            if (throwError) {
                throw new CacheLoaderException();
            }
            if (blockMillis > 0) {
                sleepMillis(blockMillis);
            }
            return "Value-" + key;
        }

        @Override
        public Map<Integer, String> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
            throw new UnsupportedOperationException();
        }

    }

}
