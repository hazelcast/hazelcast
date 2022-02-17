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

package com.hazelcast.client.cache;

import com.hazelcast.cache.CachingProviderTest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.cache.jsr.JsrTestUtil.clearCachingProviderRegistry;
import static com.hazelcast.cache.jsr.JsrTestUtil.clearSystemProperties;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientCachingProviderTest extends CachingProviderTest {

    private static final String CONFIG_CLASSPATH_LOCATION = "test-hazelcast-client-jcache.xml";
    private final List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();

    @Override
    protected String getConfigClasspathLocation() {
        return CONFIG_CLASSPATH_LOCATION;
    }

    @Override
    protected String getProviderType() {
        return "client";
    }

    @Before
    public void setup() {
        // start a member
        Config config = new Config();
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        instances.add(instance);
        // start two client instances
        instance1 = createHazelcastInstance(INSTANCE_1_NAME);
        instance2 = createHazelcastInstance(INSTANCE_2_NAME);
        try {
            instance3 = HazelcastClient.newHazelcastClient(new XmlClientConfigBuilder(CONFIG_CLASSPATH_LOCATION).build());
        } catch (IOException e) {
            throw new AssertionError("Could not construct named hazelcast client instance: " + e.getMessage());
        }
        instances.add(instance3);
        cachingProvider = createCachingProvider(instance1);
    }

    @Override
    protected HazelcastInstance createHazelcastInstance(String instanceName) {
        // Since `HazelcastClient.getHazelcastClientByName(instanceName);` doesn't work on mock client,
        // we are using real instances.
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setInstanceName(instanceName);
        HazelcastInstance instance = HazelcastClient.newHazelcastClient(clientConfig);
        instances.add(instance);
        return instance;
    }

    @Override
    protected CachingProvider createCachingProvider(HazelcastInstance defaultInstance) {
        return createClientCachingProvider(defaultInstance);
    }

    @Override
    protected void assertInstanceStarted(String instanceName) {
        HazelcastInstance otherInstance = HazelcastClient.getHazelcastClientByName(instanceName);
        assertNotNull(otherInstance);
        otherInstance.getLifecycleService().terminate();
    }

    @Override
    protected Collection<HazelcastInstance> getStartedInstances() {
        return HazelcastClient.getAllHazelcastClients();
    }

    @Override
    protected void cleanupForDefaultCacheManagerTest() {
        clearSystemProperties();
        clearCachingProviderRegistry();
    }

    @After
    public void tearDown() {
        Iterator<HazelcastInstance> iter = instances.iterator();
        while (iter.hasNext()) {
            HazelcastInstance instance = iter.next();
            try {
                instance.shutdown();
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                iter.remove();
            }
        }
        HazelcastClient.shutdownAll();
    }

}
