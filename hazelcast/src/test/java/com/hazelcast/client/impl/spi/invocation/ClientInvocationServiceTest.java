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

package com.hazelcast.client.impl.spi.invocation;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientPingCodec;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.logging.ILogger;
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
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientInvocationServiceTest extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastClientInstanceImpl client;
    private ClientInvocationServiceImpl invocationService;

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setUp() {
        hazelcastFactory.newHazelcastInstance();
        client = getHazelcastClientInstanceImpl(client);
        invocationService = (ClientInvocationServiceImpl) client.getInvocationService();
    }

    @Test
    public void testFutureIsCompleted_whenConnectionIsClosed() {
        ClientConnection connection = client.getConnectionManager().getRandomConnection();
        ClientMessage request = ClientPingCodec.encodeRequest();
        ClientInvocationFuture future = new ClientInvocationFuture(() -> false, request, mock(ILogger.class),
                invocationService.callIdSequence, Long.MAX_VALUE);
        ClientInvocation invocation = new ClientInvocation(future, (ClientInvocationServiceImpl) client.getInvocationService(),
                request, null, null, throwable -> false, Long.MAX_VALUE,
                false, false, -1, null, connection);

        //Do all the steps necessary to invoke a connection but do not put it in the write queue
        long correlationId = invocationService.callIdSequence.next();
        invocation.getClientMessage().setCorrelationId(correlationId);
        invocationService.invocations.put(correlationId, invocation);
        invocation.setSentConnection(connection);

        connection.close(null, new TargetDisconnectedException(""));
        assertTrueEventually(() -> assertTrue(future.isDone() && future.isCompletedExceptionally()));
    }

    @Test
    public void testInvocation_willNotBeNotifiedForDeadConnection_afterResponse() {
        ClientConnection connection = client.getConnectionManager().getRandomConnection();
        ClientMessage request = ClientPingCodec.encodeRequest();
        ClientInvocationFuture future = new ClientInvocationFuture(() -> false, request, mock(ILogger.class),
                invocationService.callIdSequence, Long.MAX_VALUE);
        ClientInvocation invocation = new ClientInvocation(future, (ClientInvocationServiceImpl) client.getInvocationService(),
                request, null, null, throwable -> false, Long.MAX_VALUE,
                false, false, -1, null, connection);

        //Do all the steps necessary to invoke a connection but do not put it in the write queue
        long correlationId = invocationService.callIdSequence.next();
        invocation.getClientMessage().setCorrelationId(correlationId);
        invocationService.invocations.put(correlationId, invocation);
        invocation.setSentConnection(connection);

        //Simulate a response coming back
        invocation.notify(ClientPingCodec.encodeResponse().setCorrelationId(correlationId));

        //Sent connection dies
        assertFalse(invocation.getPermissionToNotifyForDeadConnection(connection));
    }

    @Test
    public void testInvocation_willNotBeNotified_afterAConnectionIsNotifiedForDead() {
        ClientConnection connection = client.getConnectionManager().getRandomConnection();
        ClientMessage request = ClientPingCodec.encodeRequest();
        ClientInvocationFuture future = new ClientInvocationFuture(() -> false, request, mock(ILogger.class),
                invocationService.callIdSequence, Long.MAX_VALUE);
        ClientInvocation invocation = new ClientInvocation(future, (ClientInvocationServiceImpl) client.getInvocationService(),
                request, null, null, throwable -> false, Long.MAX_VALUE,
                false, false, -1, null, connection);

        //Do all the steps necessary to invoke a connection but do not put it in the write queue
        long correlationId = invocationService.callIdSequence.next();
        invocation.getClientMessage().setCorrelationId(correlationId);
        invocationService.invocations.put(correlationId, invocation);
        invocation.setSentConnection(connection);

        //Sent connection dies
        assertTrue(invocation.getPermissionToNotifyForDeadConnection(connection));
        invocationService.handleException(invocation, new TargetDisconnectedException(""), request, future);

        //Simulate a response coming back
        assertFalse(invocation.getPermissionToNotify(correlationId));
    }

    @Test
    public void testInvocation_willNotBeNotifiedForOldStalledResponse_afterARetry() {
        ClientConnection connection = client.getConnectionManager().getRandomConnection();
        ClientMessage request = ClientPingCodec.encodeRequest();
        ClientInvocationFuture future = new ClientInvocationFuture(() -> false, request, mock(ILogger.class),
                invocationService.callIdSequence, Long.MAX_VALUE);
        ClientInvocation invocation = new ClientInvocation(future, (ClientInvocationServiceImpl) client.getInvocationService(),
                request, null, null, throwable -> false, Long.MAX_VALUE,
                false, false, -1, null, connection);

        //Do all the steps necessary to invoke a connection but do not put it in the write queue
        long correlationId = invocationService.callIdSequence.next();
        invocation.getClientMessage().setCorrelationId(correlationId);
        invocationService.invocations.put(correlationId, invocation);
        invocation.setSentConnection(connection);

        //Sent connection dies
        assertTrue(invocation.getPermissionToNotifyForDeadConnection(connection));

        //simulate a retry
        invocationService.deRegisterInvocation(correlationId);
        long retryCorrelationId = invocationService.callIdSequence.next();
        invocation.getClientMessage().setCorrelationId(retryCorrelationId);
        invocationService.invocations.put(correlationId, invocation);
        invocation.setSentConnection(connection);

        //Simulate the stalled old response trying to notify the invocation
        assertFalse(invocation.getPermissionToNotify(correlationId));
    }
}
