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

package com.hazelcast.client.cache.instance;

import com.hazelcast.cache.instance.CacheThroughHazelcastInstanceTest;
import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;

import java.util.UUID;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.client.test.ClientTestSupport.makeSureDisconnectedFromServer;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheThroughHazelcastInstanceTest extends CacheThroughHazelcastInstanceTest {

    private TestHazelcastFactory instanceFactory;
    private HazelcastInstance ownerInstance;

    @Override
    protected CachingProvider createCachingProvider(HazelcastInstance instance) {
        return createClientCachingProvider(instance);
    }

    @Override
    protected HazelcastInstance createInstance() {
        if (instanceFactory != null) {
            throw new IllegalStateException("There should not be multiple creation of TestHazelcastFactory!");
        }
        instanceFactory = new TestHazelcastFactory();
        ownerInstance = instanceFactory.newHazelcastInstance();
        return instanceFactory.newHazelcastClient();
    }

    @Override
    protected HazelcastInstance createInstance(Config config) {
        if (instanceFactory != null) {
            throw new IllegalStateException("There should not be multiple creation of TestHazelcastFactory!");
        }
        instanceFactory = new TestHazelcastFactory();
        ownerInstance = instanceFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(10_000);

        if (config.getClassLoader() != null) {
            final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
            try {
                clientConfig.setClassLoader(config.getClassLoader());
                Thread.currentThread().setContextClassLoader(config.getClassLoader());
                return instanceFactory.newHazelcastClient(clientConfig);
            } finally {
                Thread.currentThread().setContextClassLoader(tccl);
            }
        } else {
            return instanceFactory.newHazelcastClient(clientConfig);
        }
    }

    @Override
    protected void shutdownOwnerInstance(HazelcastInstance instance) {
        if (ownerInstance != null) {
            UUID memberUuid = ownerInstance.getLocalEndpoint().getUuid();
            ownerInstance.shutdown();
            makeSureDisconnectedFromServer(instance, memberUuid);
        } else {
            throw new IllegalStateException("");
        }
    }

    @Override
    protected Class<? extends Exception> getInstanceNotActiveExceptionType() {
        return HazelcastClientNotActiveException.class;
    }

    @After
    public void tearDown() {
        if (instanceFactory != null) {
            ownerInstance = null;
            instanceFactory.shutdownAll();
            instanceFactory = null;
        }
    }

}
