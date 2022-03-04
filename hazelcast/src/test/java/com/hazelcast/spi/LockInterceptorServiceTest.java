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

package com.hazelcast.spi;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.locksupport.LockProxySupport;
import com.hazelcast.internal.locksupport.LockStoreInfo;
import com.hazelcast.internal.locksupport.LockSupportService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.LockInterceptorService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LockInterceptorServiceTest extends HazelcastTestSupport {

    private static final ConcurrentMap<String, AtomicInteger> LOCK_COUNTER = new ConcurrentHashMap<>();

    @Before
    public void setup() {
        LOCK_COUNTER.clear();
    }

    @Test
    public void testLockInterceptorServiceIsConsulted() {
        LockInterceptingService implementation = new LockInterceptingService(false);

        testLockingInterceptor(implementation);

        assertLockCount(1);
    }

    @Test
    public void testObjectIsNotLocked_whenLockInterceptorThrowsException() {
        LockInterceptingService implementation = new LockInterceptingService(true);

        testLockingInterceptor(implementation);

        assertLockCount(0);
    }

    private void testLockingInterceptor(LockInterceptingService implementation) {
        Config config = new Config();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(new ServiceConfig()
                .setEnabled(true)
                .setName(LockInterceptingService.SERVICE_NAME)
                .setImplementation(implementation));

        HazelcastInstance member = createHazelcastInstance(config);
        NodeEngine nodeEngine = getNodeEngineImpl(member);
        implementation.serializationService = getSerializationService(member);

        LockProxySupport lockProxySupport = new LockProxySupport(
                new DistributedObjectNamespace(LockInterceptingService.SERVICE_NAME, "test-object"), 10000);

        for (int i = 0; i < 100; i++) {
            try {
                Data key = getSerializationService(member).toData("key" + i);
                lockProxySupport.lock(nodeEngine, key);
            } catch (RuntimeException e) {
                ignore(e);
            }
        }
    }

    private void assertLockCount(int expectedCount) {
        for (int i = 0; i < 100; i++) {
            assertEquals(expectedCount, LOCK_COUNTER.get("key" + i).get());
        }
    }

    public static class LockInterceptingService implements LockInterceptorService<Data>, ManagedService {

        public static final String SERVICE_NAME = "test-lock-intercepting-service";

        private final boolean throwException;
        private volatile SerializationService serializationService;

        public LockInterceptingService(boolean throwException) {
            this.throwException = throwException;
        }

        @Override
        public void onBeforeLock(String distributedObjectName, Data key) {
            String stringKey = serializationService.toObject(key);
            AtomicInteger counter = getOrPutIfAbsent(LOCK_COUNTER, stringKey, arg -> new AtomicInteger());
            if (throwException) {
                throw new RuntimeException("failed");
            }
            counter.getAndIncrement();
        }

        @Override
        public void init(NodeEngine nodeEngine, Properties properties) {
            final LockSupportService lockService = nodeEngine.getServiceOrNull(LockSupportService.SERVICE_NAME);
            if (lockService != null) {
                lockService.registerLockStoreConstructor(SERVICE_NAME, new LockStoreInfoConstructor());
            }
        }

        @Override
        public void reset() {
        }

        @Override
        public void shutdown(boolean terminate) {
        }
    }

    public static class LockStoreInfoConstructor implements ConstructorFunction<ObjectNamespace, LockStoreInfo> {
        @Override
        public LockStoreInfo createNew(ObjectNamespace arg) {
            return new LockStoreInfo() {
                @Override
                public int getBackupCount() {
                    return 0;
                }

                @Override
                public int getAsyncBackupCount() {
                    return 0;
                }
            };
        }
    }

}
