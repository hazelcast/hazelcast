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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.protocol.codec.ClientPingCodec;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.spi.exception.TargetDisconnectedException;
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

    private ClientConnection closedConnection() {
        ClientConnection conn = mock(ClientConnection.class, RETURNS_DEEP_STUBS);
        when(conn.isAlive()).thenReturn(false);
        return conn;
    }

    @Test
    public void testInvocation_willNotBeNotifiedForDeadConnection_afterResponse() {
        ClientConnection connection = client.getConnectionManager().getRandomConnection();
        ClientInvocation invocation = new ClientInvocation(client, ClientPingCodec.encodeRequest(), null, connection);

        //Do all the steps necessary to invoke a connection but do not put it in the write queue
        ClientInvocationServiceImpl invocationService = (ClientInvocationServiceImpl) client.getInvocationService();
        long correlationId = invocationService.getCallIdSequence().next();
        invocation.getClientMessage().setCorrelationId(correlationId);
        invocationService.registerInvocation(invocation, connection);
        invocation.setSentConnection(connection);

        //Simulate a response coming back
        invocation.notify(ClientPingCodec.encodeResponse().setCorrelationId(correlationId));

        //Sent connection dies
        assertFalse(invocation.getPermissionToNotifyForDeadConnection(connection));
    }

    @Test
    public void testInvocation_willNotBeNotified_afterAConnectionIsNotifiedForDead() {
        ClientConnection connection = client.getConnectionManager().getRandomConnection();
        ClientInvocation invocation = new ClientInvocation(client, ClientPingCodec.encodeRequest(), null, connection);

        //Do all the steps necessary to invoke a connection but do not put it in the write queue
        ClientInvocationServiceImpl invocationService = (ClientInvocationServiceImpl) client.getInvocationService();
        long correlationId = invocationService.getCallIdSequence().next();
        invocation.getClientMessage().setCorrelationId(correlationId);
        invocationService.registerInvocation(invocation, connection);
        invocation.setSentConnection(connection);

        //Sent connection dies
        assertTrue(invocation.getPermissionToNotifyForDeadConnection(connection));
        invocation.notifyExceptionWithOwnedPermission(new TargetDisconnectedException(""));

        //Simulate a response coming back
        assertFalse(invocation.getPermissionToNotify(correlationId));
    }

    @Test
    public void testInvocation_willNotBeNotifiedForOldStalledResponse_afterARetry() {
        ClientConnection connection = client.getConnectionManager().getRandomConnection();
        ClientInvocation invocation = new ClientInvocation(client, ClientPingCodec.encodeRequest(), null, connection);

        //Do all the steps necessary to invoke a connection but do not put it in the write queue
        ClientInvocationServiceImpl invocationService = (ClientInvocationServiceImpl) client.getInvocationService();
        long correlationId = invocationService.getCallIdSequence().next();
        invocation.getClientMessage().setCorrelationId(correlationId);
        invocationService.registerInvocation(invocation, connection);
        invocation.setSentConnection(connection);

        //Sent connection dies
        assertTrue(invocation.getPermissionToNotifyForDeadConnection(connection));

        //simulate a retry
        invocationService.deRegisterInvocation(correlationId);
        long retryCorrelationId = invocationService.getCallIdSequence().next();
        invocation.getClientMessage().setCorrelationId(retryCorrelationId);
        invocationService.registerInvocation(invocation, connection);
        invocation.setSentConnection(connection);

        //Simulate the stalled old response trying to notify the invocation
        assertFalse(invocation.getPermissionToNotify(correlationId));
    }
}
