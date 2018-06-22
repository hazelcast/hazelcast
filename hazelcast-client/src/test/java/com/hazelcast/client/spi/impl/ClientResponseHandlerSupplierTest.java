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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.ClientResponseHandlerSupplier.AsyncMultiThreadedResponseHandler;
import com.hazelcast.client.spi.impl.ClientResponseHandlerSupplier.AsyncSingleThreadedResponseHandler;
import com.hazelcast.client.spi.impl.ClientResponseHandlerSupplier.SyncResponseHandler;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.client.spi.properties.ClientProperty.RESPONSE_THREAD_COUNT;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientResponseHandlerSupplierTest extends ClientTestSupport {
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Before
    public void before() {
        hazelcastFactory.newHazelcastInstance();
    }

    @After
    public void after() {
        hazelcastFactory.terminateAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenNegativeResponseThreads() {
        getResponseHandler(-1);
    }

    @Test
    public void whenZeroResponseThreads() {
        ClientResponseHandler handler = getResponseHandler(0);
        assertInstanceOf(SyncResponseHandler.class, handler);
    }

    @Test
    public void whenOneResponseThreads() {
        ClientResponseHandler handler = getResponseHandler(1);
        assertInstanceOf(AsyncSingleThreadedResponseHandler.class, handler);
    }

    @Test
    public void whenMultipleResponseThreads() {
        ClientResponseHandler handler = getResponseHandler(2);
        assertInstanceOf(AsyncMultiThreadedResponseHandler.class, handler);
    }

    private ClientResponseHandler getResponseHandler(int threadCount) {
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(
                new ClientConfig()
                        .setProperty(RESPONSE_THREAD_COUNT.getName(), "" + threadCount));
        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        AbstractClientInvocationService invocationService = (AbstractClientInvocationService) clientInstanceImpl.getInvocationService();

        ClientResponseHandlerSupplier responseHandlerSupplier = new ClientResponseHandlerSupplier(invocationService);
        return responseHandlerSupplier.get();
    }
}
