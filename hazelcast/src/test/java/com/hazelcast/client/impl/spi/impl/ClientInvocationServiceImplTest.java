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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.Config;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.impl.protocol.ClientMessage.CORRELATION_ID_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.UNFRAGMENTED_MESSAGE;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueAllTheTime;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientInvocationServiceImplTest {

    private static final int MOCK_FRAME_LENGTH = CORRELATION_ID_FIELD_OFFSET + Bits.LONG_SIZE_IN_BYTES;

    private ClientInvocationServiceImpl invocationService;
    private HazelcastClientInstanceImpl client;
    private SingleThreadedTaskScheduler taskScheduler;

    @Before
    public void setUp() {
        client = mock(HazelcastClientInstanceImpl.class, RETURNS_DEEP_STUBS);
        when(client.getProperties()).thenReturn(new HazelcastProperties(new Config()));
        taskScheduler = new SingleThreadedTaskScheduler();
        when(client.getTaskScheduler()).thenReturn(taskScheduler);
        invocationService = new ClientInvocationServiceImpl(client);
        when(client.getInvocationService()).thenReturn(invocationService);
    }

    @After
    public void tearDown() {
        invocationService.shutdown();
        taskScheduler.shutdown();
    }

    @Test
    public void testCleanResourcesTask_rejectsPendingInvocationsWithClosedConnections() {
        invocationService.start();

        ClientConnection conn = prepareConnection(false);
        ClientInvocation invocation = prepareInvocation(1);
        invocation.setSendConnection(conn);
        invocationService.registerInvocation(invocation, conn);

        ClientInvocationFuture future = invocation.getClientInvocationFuture();
        assertTrueEventually(() -> assertTrue(future.isDone() && future.isCompletedExceptionally()));
    }

    @Test
    public void testCleanResourcesTask_ignoresPendingInvocationsWithAliveConnections() {
        invocationService.start();

        ClientConnection closedConn = prepareConnection(false);
        ClientInvocation invocation1 = prepareInvocation(1);
        invocation1.setSendConnection(closedConn);
        invocationService.registerInvocation(invocation1, closedConn);

        ClientConnection aliveConn = prepareConnection(true);
        ClientInvocation invocation2 = prepareInvocation(2);
        invocationService.registerInvocation(invocation2, aliveConn);

        ClientInvocationFuture future1 = invocation1.getClientInvocationFuture();
        ClientInvocationFuture future2 = invocation2.getClientInvocationFuture();
        assertTrueEventually(() -> assertTrue(future1.isDone() && future1.isCompletedExceptionally()));
        assertTrueAllTheTime(() -> assertFalse(future2.isDone()), 1);
    }

    private ClientInvocation prepareInvocation(long correlationId) {
        ClientMessage message = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame =
                new ClientMessage.Frame(new byte[MOCK_FRAME_LENGTH], UNFRAGMENTED_MESSAGE);
        message.add(initialFrame);
        message.setCorrelationId(correlationId);
        return new ClientInvocation(client, message, null);
    }

    private ClientConnection prepareConnection(boolean alive) {
        ClientConnection conn = mock(ClientConnection.class, RETURNS_DEEP_STUBS);
        when(conn.isAlive()).thenReturn(alive);
        return conn;
    }

    private static class SingleThreadedTaskScheduler implements TaskScheduler {

        private final ScheduledExecutorService internalExecutor = Executors.newScheduledThreadPool(1);

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return internalExecutor.schedule(command, delay, unit);
        }

        @Override
        public <V> ScheduledFuture<Future<V>> schedule(Callable<V> command, long delay, TimeUnit unit) {
            return (ScheduledFuture<Future<V>>) internalExecutor.schedule(command, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long period, TimeUnit unit) {
            return internalExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public void execute(Runnable command) {
            internalExecutor.execute(command);
        }

        public void shutdown() {
            internalExecutor.shutdownNow();
        }
    }
}
