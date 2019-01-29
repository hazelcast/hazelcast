/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cluster;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.mocknetwork.MockNodeContext;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientNodeExtensionTest
        extends HazelcastTestSupport {
    private TestHazelcastFactory factory;

    @Before
    public void before()
            throws IOException {
        factory = new TestHazelcastFactory();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    private class ManagedExtensionNodeContext
            extends MockNodeContext {
        private AtomicBoolean startupDone;

        protected ManagedExtensionNodeContext(TestNodeRegistry registry, Address thisAddress, boolean isStarted) {
            super(registry, thisAddress);
            startupDone = new AtomicBoolean(isStarted);
        }

        @Override
        public NodeExtension createNodeExtension(Node node) {
            return new DefaultNodeExtension(node) {
                @Override
                public boolean isStartCompleted() {
                    return startupDone.get() && super.isStartCompleted();
                }
            };
        }

        public void setStartupDone(boolean started) {
            this.startupDone.set(started);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void test_canNotConnect_whenNodeExtensionIsNotComplete()
            throws UnknownHostException {
        HazelcastInstanceFactory.newHazelcastInstance(new Config(), randomName(),
                new ManagedExtensionNodeContext(factory.getRegistry(), new Address("127.0.0.1", 5555), false));

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5555");
        factory.newHazelcastClient(clientConfig);
    }

    @Test(expected = OperationTimeoutException.class)
    public void test_canGetFromMap_whenNodeExtensionIsNotComplete() {
        IMap<Object, Object> map = null;
        ManagedExtensionNodeContext nodeContext = null;
        try {
            nodeContext = new ManagedExtensionNodeContext(factory.getRegistry(), new Address("127.0.0.1", 5555), true);

            HazelcastInstanceFactory.newHazelcastInstance(new Config(), randomName(), nodeContext);

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
            clientConfig.getNetworkConfig().addAddress("127.0.0.1:5555");

            HazelcastInstance hazelcastClient = factory.newHazelcastClient(clientConfig);

            map = hazelcastClient.getMap(randomMapName());

            assertNull(map.get("dummy"));
        } catch (Throwable t) {
            fail("Should not throw exception! Error:" + t);
        }

        nodeContext.setStartupDone(false);

        map.get("dummy");
    }
}
