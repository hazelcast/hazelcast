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

package com.hazelcast.client.spi;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.ClientProxyFactoryWithContext;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ProxyFactoryTest {

    private static final String SERVICE_NAME_WITH_CONTEXT = CustomServiceWithContext.class.getSimpleName();
    private static final String SERVICE_NAME = CustomService.class.getSimpleName();

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private ClientContext context;

    @Before
    public void setup() {
        ServiceConfig serviceConfigWithContext = new ServiceConfig()
                .setEnabled(true)
                .setName(SERVICE_NAME_WITH_CONTEXT)
                .setImplementation(new CustomServiceWithContext());

        ServiceConfig serviceConfig = new ServiceConfig()
                .setEnabled(true)
                .setName(SERVICE_NAME)
                .setImplementation(new CustomService());

        Config config = new Config();
        config.getServicesConfig()
                .addServiceConfig(serviceConfigWithContext)
                .addServiceConfig(serviceConfig);

        hazelcastFactory.newHazelcastInstance(config);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testCustomProxyWithContext_usingFactoryClassName() {
        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig()
                .setService(SERVICE_NAME_WITH_CONTEXT)
                .setClassName(CustomProxyFactoryWithContext.class.getName());

        ClientConfig clientConfig = new ClientConfig()
                .addProxyFactoryConfig(proxyFactoryConfig);

        testProxyCreation(SERVICE_NAME_WITH_CONTEXT, clientConfig);
    }

    @Test
    public void testCustomProxy_usingFactoryClassName() {
        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig()
                .setService(SERVICE_NAME)
                .setClassName(CustomProxyFactory.class.getName());

        ClientConfig clientConfig = new ClientConfig()
                .addProxyFactoryConfig(proxyFactoryConfig);

        testProxyCreation(SERVICE_NAME, clientConfig);
    }

    @Test
    public void testCustomProxyWithContext_usingFactoryImplementation() {
        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig()
                .setService(SERVICE_NAME_WITH_CONTEXT)
                .setFactoryImpl(new CustomProxyFactoryWithContext());

        ClientConfig clientConfig = new ClientConfig()
                .addProxyFactoryConfig(proxyFactoryConfig);

        testProxyCreation(SERVICE_NAME_WITH_CONTEXT, clientConfig);
    }

    @Test
    public void testCustomProxy_usingFactoryImplementation() {
        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig()
                .setService(SERVICE_NAME)
                .setFactoryImpl(new CustomProxyFactory());

        ClientConfig clientConfig = new ClientConfig()
                .addProxyFactoryConfig(proxyFactoryConfig);

        testProxyCreation(SERVICE_NAME, clientConfig);
    }

    @Test(expected = ExpectedError.class)
    public void testClientProxyFactoryWithContext_whenInitThrowsError() {
        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig()
                .setService(SERVICE_NAME_WITH_CONTEXT)
                .setFactoryImpl(new ClientProxyFactoryWithContext() {
                    @Override
                    public ClientProxy create(String id, ClientContext context) {
                        return new ClientProxy(SERVICE_NAME_WITH_CONTEXT, id, context) {
                            @Override
                            protected void onInitialize() {
                                super.onInitialize();
                                throw new ExpectedError();
                            }
                        };
                    }
                });

        ClientConfig clientConfig = new ClientConfig()
                .addProxyFactoryConfig(proxyFactoryConfig);

        testProxyCreation(SERVICE_NAME_WITH_CONTEXT, clientConfig);
    }

    @Test(expected = ExpectedError.class)
    @SuppressWarnings("deprecation")
    public void testClientProxyFactory_whenInitThrowsError() {
        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig()
                .setService(SERVICE_NAME)
                .setFactoryImpl(new ClientProxyFactory() {
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

        ClientConfig clientConfig = new ClientConfig()
                .addProxyFactoryConfig(proxyFactoryConfig);

        testProxyCreation(SERVICE_NAME, clientConfig);
    }

    private void testProxyCreation(String serviceName, ClientConfig clientConfig) {
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        context = clientInstanceImpl.getProxyManager().getContext();

        ClientProxy proxy = client.getDistributedObject(serviceName, "CustomClientProxy");

        assertEquals(serviceName, proxy.getServiceName());
        assertEquals("CustomClientProxy", proxy.getName());
    }

    private class CustomServiceWithContext implements RemoteService {

        @Override
        public DistributedObject createDistributedObject(String objectName) {
            return new CustomClientProxyWithContext(SERVICE_NAME_WITH_CONTEXT, objectName, context);
        }

        @Override
        public void destroyDistributedObject(String objectName) {
        }
    }

    private static class CustomProxyFactoryWithContext extends ClientProxyFactoryWithContext {

        @Override
        public ClientProxy create(String id, ClientContext context) {
            return new CustomClientProxyWithContext(SERVICE_NAME_WITH_CONTEXT, id, context);
        }
    }

    private static class CustomClientProxyWithContext extends ClientProxy {

        protected CustomClientProxyWithContext(String serviceName, String objectName, ClientContext context) {
            super(serviceName, objectName, context);
        }
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

    @SuppressWarnings("deprecation")
    private static class CustomClientProxy extends ClientProxy {

        protected CustomClientProxy(String serviceName, String objectName) {
            super(serviceName, objectName);
        }
    }

    private static class ExpectedError extends Error {
    }
}
