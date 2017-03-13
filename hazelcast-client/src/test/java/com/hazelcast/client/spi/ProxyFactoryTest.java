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

package com.hazelcast.client.spi;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ProxyFactoryTest {

    private static final String SERVICE_NAME = "CustomService";

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        Config config = new Config();
        ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setEnabled(true).setName(SERVICE_NAME)
                .setImplementation(new CustomService());
        config.getServicesConfig().addServiceConfig(serviceConfig);
        hazelcastFactory.newHazelcastInstance(config);
    }

    @Test
    public void testCustomProxy_usingFactoryClassName() {
        ClientConfig clientConfig = new ClientConfig();
        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig();
        proxyFactoryConfig.setService(SERVICE_NAME);
        proxyFactoryConfig.setClassName(CustomProxyFactory.class.getName());
        clientConfig.addProxyFactoryConfig(proxyFactoryConfig);

        testCustomProxy(clientConfig);
    }

    @Test
    public void testCustomProxy_usingFactoryImplementation() {
        ClientConfig clientConfig = new ClientConfig();
        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig();
        proxyFactoryConfig.setService(SERVICE_NAME);
        proxyFactoryConfig.setFactoryImpl(new CustomProxyFactory());
        clientConfig.addProxyFactoryConfig(proxyFactoryConfig);

        testCustomProxy(clientConfig);
    }

    private void testCustomProxy(ClientConfig clientConfig) {
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        String objectName = "custom-object";
        CustomClientProxy proxy = client.getDistributedObject(SERVICE_NAME, objectName);
        Assert.assertEquals(SERVICE_NAME, proxy.getServiceName());
        Assert.assertEquals(objectName, proxy.getName());
    }

    @Test
    public void testProxy_whenInitThrowsError() {
        ClientConfig clientConfig = new ClientConfig();
        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig();
        proxyFactoryConfig.setService(SERVICE_NAME);
        proxyFactoryConfig.setFactoryImpl(new ClientProxyFactory() {
            @Override
            public ClientProxy create(String id) {
                return new ClientProxy(SERVICE_NAME, id) {
                    @Override
                    protected void onInitialize() {
                        super.onInitialize();
                        throw new ExpectedError();
                    }
                };
            }
        });

        clientConfig.addProxyFactoryConfig(proxyFactoryConfig);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        String objectName = "custom-object";
        try {
            client.getDistributedObject(SERVICE_NAME, objectName);
            fail("Client proxy initialization should fail!");
        } catch (ExpectedError expected) {
        }
    }

    private static class ExpectedError extends Error {
    }

    private static class CustomService implements RemoteService {
        @Override
        public DistributedObject createDistributedObject(String objectName) {
            return new CustomClientProxy(SERVICE_NAME, objectName);
        }

        @Override
        public void destroyDistributedObject(String objectName) {
        }
    }

    private static class CustomProxyFactory implements ClientProxyFactory {

        @Override
        public ClientProxy create(String id) {
            return new CustomClientProxy(SERVICE_NAME, id);
        }
    }

    private static class CustomClientProxy extends ClientProxy {

        protected CustomClientProxy(String serviceName, String objectName) {
            super(serviceName, objectName);
        }
    }
}
