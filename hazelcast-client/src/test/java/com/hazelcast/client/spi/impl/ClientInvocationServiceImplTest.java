/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToAddressCodec;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.executor.ExecutorServiceTestSupport;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientInvocationServiceImplTest extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastClientInstanceImpl client;

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setUp() {
        hazelcastFactory.newHazelcastInstance();
        client = getHazelcastClientInstanceImpl(hazelcastFactory.newHazelcastClient());
    }

    @Test(expected = TargetDisconnectedException.class)
    public void testCleanResourcesTask_rejectsPendingInvocationsWithClosedConnections() throws Throwable {
        ClientConnection connection = client.getConnectionManager().getActiveConnections().iterator().next();

        ExecutorServiceTestSupport.SleepingTask sleepingTask = new ExecutorServiceTestSupport.SleepingTask(Integer.MAX_VALUE);
        Data sleepingTaskData = client.getSerializationService().toData(sleepingTask);
        ClientMessage request = ExecutorServiceSubmitToAddressCodec.encodeRequest("name", "uuid", sleepingTaskData, connection.getEndPoint());

        ClientInvocation invocation = new ClientInvocation(client, request, null, connection);
        ClientInvocationFuture future = invocation.invoke();
        connection.close(null, null);

        try {
            future.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }
}
