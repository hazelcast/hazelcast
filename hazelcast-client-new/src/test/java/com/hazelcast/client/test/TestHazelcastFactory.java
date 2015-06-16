/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.impl.ClientServiceFactory;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.nio.Address;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestHazelcastFactory extends TestHazelcastInstanceFactory {

    private final boolean mockNetwork = TestEnvironment.isMockNetwork();
    private final List<HazelcastClientInstanceImpl> clients = new ArrayList<HazelcastClientInstanceImpl>(10);
    private final TestClientRegistry clientRegistry;
    private final AtomicInteger clientPorts = new AtomicInteger(6000);

    public TestHazelcastFactory() {
        super(0);
        this.clientRegistry = new TestClientRegistry(registry);
    }

    public HazelcastInstance newHazelcastClient() {
        return this.newHazelcastClient(null);
    }

    public HazelcastInstance newHazelcastClient(ClientConfig config) {
        if (!mockNetwork) {
            return HazelcastClient.newHazelcastClient(config);
        }

        if (config == null) {
            config = new XmlClientConfigBuilder().build();
        }
        for (Address address : addresses) {
            config.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
        }

        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        HazelcastClientProxy proxy;
        try {
            Thread.currentThread().setContextClassLoader(HazelcastClient.class.getClassLoader());
            ClientServiceFactory clientServiceFactory =
                    clientRegistry.createClientServiceFactory(createAddress("127.0.0.1", clientPorts.incrementAndGet()));
            final HazelcastClientInstanceImpl client = new HazelcastClientInstanceImpl(config, clientServiceFactory);
            client.start();
            clients.add(client);
            OutOfMemoryErrorDispatcher.registerClient(client);
            proxy = new HazelcastClientProxy(client);
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
        return proxy;
    }

    @Override
    public void shutdownAll() {
        if (!mockNetwork) {
            HazelcastClient.shutdownAll();
        } else {
            for (HazelcastClientInstanceImpl client : clients) {
                client.shutdown();
            }
        }
        super.shutdownAll();
    }

    @Override
    public void terminateAll() {
        if (!mockNetwork) {
            //For client terminateAll and shutdownAll is same
            HazelcastClient.shutdownAll();
        } else {
            for (HazelcastClientInstanceImpl client : clients) {
                client.getLifecycleService().terminate();
            }
        }
        super.terminateAll();
    }
}
