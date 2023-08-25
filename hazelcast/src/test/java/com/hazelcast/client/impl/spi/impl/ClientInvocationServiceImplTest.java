/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientPingCodec;
import com.hazelcast.client.impl.protocol.codec.SetAddCodec;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientInvocationServiceImplTest extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance member;
    private HazelcastClientInstanceImpl client;

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setUp() {
        member = hazelcastFactory.newHazelcastInstance();
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
        ClientConnection conn = mock(ClientConnection.class);
        when(conn.isAlive()).thenReturn(false);
        return conn;
    }

    private ClientConnection mockConnection() {
        ClientConnection connection = mock(ClientConnection.class);
        when(connection.write(any())).thenReturn(true);
        return connection;
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

    @Test
    public void testInvokeUrgent_whenThereAreNoCompactSchemas_andClientIsInitializedOnCluster() {
        // No compact schemas, no need to check urgent invocations
        assertFalse(client.shouldCheckUrgentInvocations());

        // client is connected to the member and initialized on it since this
        // is the initial cluster connection
        assertTrue(client.getConnectionManager().clientInitializedOnCluster());

        ClientConnection connection = mockConnection();

        // Urgent invocations should be done
        ClientInvocation pingInvocation = checkUrgentInvocation_withNoData(connection);
        ClientInvocation setAddInvocation = checkUrgentInvocation_withData(connection);

        verify(connection, times(1)).write(pingInvocation.getClientMessage());
        verify(connection, times(1)).write(setAddInvocation.getClientMessage());
    }

    @Test
    public void testInvokeUrgent_whenThereAreCompactSchemas_andClientIsInitializedOnCluster() {
        client.getMap("testMap").put("test", new EmployeeDTO());

        // Some compact schemas, need to check urgent invocations
        assertTrue(client.shouldCheckUrgentInvocations());

        // client is connected to the member and initialized on it since this
        // is the initial cluster connection
        assertTrue(client.getConnectionManager().clientInitializedOnCluster());

        ClientConnection connection = mockConnection();

        // Urgent invocations should be done
        ClientInvocation pingInvocation = checkUrgentInvocation_withNoData(connection);
        ClientInvocation setAddInvocation = checkUrgentInvocation_withData(connection);

        verify(connection, times(1)).write(pingInvocation.getClientMessage());
        verify(connection, times(1)).write(setAddInvocation.getClientMessage());
    }

    @Test
    public void testInvokeUrgent_whenThereAreNoCompactSchemas_andClientIsNotInitializedOnCluster() {
        UUID memberUuid = member.getLocalEndpoint().getUuid();
        member.shutdown();
        makeSureDisconnectedFromServer(client, memberUuid);

        // No compact schemas, no need to check urgent invocations
        assertFalse(client.shouldCheckUrgentInvocations());

        // client is disconnected, so not initialized on cluster
        assertTrueEventually(() -> assertFalse(client.getConnectionManager().clientInitializedOnCluster()));

        ClientConnection connection = mockConnection();

        // Urgent invocations should be done
        ClientInvocation pingInvocation = checkUrgentInvocation_withNoData(connection);
        ClientInvocation setAddInvocation = checkUrgentInvocation_withData(connection);

        verify(connection, times(1)).write(pingInvocation.getClientMessage());
        verify(connection, times(1)).write(setAddInvocation.getClientMessage());
    }

    @Test
    public void testInvokeUrgent_whenThereAreCompactSchemas_andClientIsNotInitializedOnCluster() {
        client.getMap("testMap").put("test", new EmployeeDTO());

        UUID memberUuid = member.getLocalEndpoint().getUuid();
        member.shutdown();
        makeSureDisconnectedFromServer(client, memberUuid);

        // Some compact schemas, need to check urgent invocations
        assertTrue(client.shouldCheckUrgentInvocations());

        // client is disconnected, so not initialized on cluster
        assertTrueEventually(() -> assertFalse(client.getConnectionManager().clientInitializedOnCluster()));

        ClientConnection connection = mockConnection();

        // Urgent invocations should be done, if they contain no data
        ClientInvocation pingInvocation = checkUrgentInvocation_withNoData(connection);
        ClientInvocation setAddInvocation = checkUrgentInvocation_withData(connection);

        verify(connection, times(1)).write(pingInvocation.getClientMessage());
        verify(connection, never()).write(setAddInvocation.getClientMessage());
    }


    private ClientInvocation checkUrgentInvocation_withNoData(ClientConnection connection) {
        ClientMessage request = ClientPingCodec.encodeRequest();
        assertFalse(request.isContainsSerializedDataInRequest());
        ClientInvocation invocation = new ClientInvocation(client, request, null, connection);
        invocation.invokeUrgent();
        return invocation;
    }

    private ClientInvocation checkUrgentInvocation_withData(ClientConnection connection) {
        Data data = client.getSerializationService().toData("test");
        ClientMessage request = SetAddCodec.encodeRequest("test", data);
        assertTrue(request.isContainsSerializedDataInRequest());
        ClientInvocation invocation = new ClientInvocation(client, request, null, connection);
        invocation.invokeUrgent();
        return invocation;
    }
}
