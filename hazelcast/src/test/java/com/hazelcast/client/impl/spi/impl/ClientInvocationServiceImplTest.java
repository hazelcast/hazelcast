/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.protocol.codec.ClientPingCodec;
import com.hazelcast.client.impl.protocol.codec.MapSizeCodec;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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

    @Test
    public void testCleanResourcesTask_rejectsPendingInvocationsWithClosedConnections() {
        ClientConnection closedConn = closedConnection();
        ClientInvocation invocation = new ClientInvocation(client, ClientPingCodec.encodeRequest(), null, closedConn);

        ClientInvocationFuture future = invocation.invoke();
        assertTrueEventually(() -> assertTrue(future.isDone() && future.isCompletedExceptionally()));
    }

    @Test
    public void testCleanResourcesTask_ignoresPendingInvocationsWithAliveConnections() {
        ClientConnection closedConn = closedConnection();
        ClientInvocation invocation1 = new ClientInvocation(client, ClientPingCodec.encodeRequest(), null, closedConn);
        ClientInvocationFuture future1 = invocation1.invoke();

        ClientConnection connection = client.getConnectionManager().getRandomConnection();
        ClientInvocation invocation2 = new ClientInvocation(client, ClientPingCodec.encodeRequest(), null, connection);
        ClientInvocationFuture future2 = invocation2.invoke();

        assertTrueEventually(() -> assertTrue(future1.isDone() && future1.isCompletedExceptionally()));
        assertTrueAllTheTime(() -> assertFalse(future2.isDone()), 1);
    }

    @Test
    public void testInvocation_willNotBeRetried_afterFirstNotify() {
        ClientConnection connection = client.getConnectionManager().getRandomConnection();
        ClientInvocation invocation = new ClientInvocation(client, ClientPingCodec.encodeRequest(), null, connection);
        ClientInvocation spiedInvocation = spy(invocation);

        spiedInvocation.invoke();
        spiedInvocation.notify(MapSizeCodec.encodeResponse(4));
        spiedInvocation.notifyException(new RetryableHazelcastException());

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(spiedInvocation, times(0)).run();
            }
        }, 1);

    }

    @Test
    public void testInvocation_willNotBeRetried_afterFirstNotifyException() {
        ClientConnection connection = client.getConnectionManager().getRandomConnection();
        ClientInvocation invocation = new ClientInvocation(client, ClientPingCodec.encodeRequest(), null, connection);
        ClientInvocation spiedInvocation = spy(invocation);

        spiedInvocation.invoke();
        spiedInvocation.notifyException(new IllegalStateException());
        spiedInvocation.notifyException(new RetryableHazelcastException());

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(spiedInvocation, times(0)).run();
            }
        }, 1);

    }

    private ClientConnection closedConnection() {
        ClientConnection conn = mock(ClientConnection.class, RETURNS_DEEP_STUBS);
        when(conn.isAlive()).thenReturn(false);
        return conn;
    }
}
