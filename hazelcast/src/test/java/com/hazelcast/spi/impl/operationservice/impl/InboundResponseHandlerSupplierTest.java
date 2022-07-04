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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InboundResponseHandlerSupplier.AsyncMultithreadedResponseHandler;
import com.hazelcast.spi.impl.operationservice.impl.InboundResponseHandlerSupplier.AsyncSingleThreadedResponseHandler;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.sequence.CallIdSequenceWithoutBackpressure;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;
import java.util.function.Consumer;

import static com.hazelcast.internal.nio.Packet.FLAG_OP_RESPONSE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InboundResponseHandlerSupplierTest extends HazelcastTestSupport {

    private InternalSerializationService serializationService;
    private InvocationRegistry invocationRegistry;
    private NodeEngine nodeEngine;
    private InboundResponseHandlerSupplier supplier;

    @Before
    public void setup() {
        ILogger logger = Logger.getLogger(getClass());
        HazelcastProperties properties = new HazelcastProperties(new Properties());
        invocationRegistry = new InvocationRegistry(logger, new CallIdSequenceWithoutBackpressure(), properties);
        serializationService = new DefaultSerializationServiceBuilder().build();
        nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getLogger(any(Class.class))).thenReturn(logger);
        when(nodeEngine.getSerializationService()).thenReturn(serializationService);
    }

    @After
    public void after() {
        if (supplier != null) {
            supplier.shutdown();
        }
    }

    @Test
    public void get_whenZeroResponseThreads() {
        supplier = newSupplier(0);
        assertInstanceOf(InboundResponseHandler.class, supplier.get());
    }

    @Test
    public void get_whenResponseThreads() {
        supplier = newSupplier(1);
        assertInstanceOf(AsyncSingleThreadedResponseHandler.class, supplier.get());
    }

    @Test
    public void get_whenMultipleResponseThreads() {
        supplier = newSupplier(2);
        assertInstanceOf(AsyncMultithreadedResponseHandler.class, supplier.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void get_whenNegativeResponseThreads() {
        newSupplier(-1);
    }

    private InboundResponseHandlerSupplier newSupplier(int threadCount) {
        Properties props = new Properties();
        props.put(ClusterProperty.RESPONSE_THREAD_COUNT.getName(), "" + threadCount);
        HazelcastProperties properties = new HazelcastProperties(props);
        when(nodeEngine.getProperties()).thenReturn(properties);

        return new InboundResponseHandlerSupplier(
                getClass().getClassLoader(), invocationRegistry, "hz", nodeEngine);
    }

    @Test
    public void whenNoProblemPacket_andZeroResponseThreads() {
        whenNoProblemPacket(0);
    }

    @Test
    public void whenNoProblemPacket_andOneResponseThreads() {
        whenNoProblemPacket(1);
    }

    @Test
    public void whenNoProblemPacket_andMultipleResponseThreads() {
        whenNoProblemPacket(2);
    }

    private void whenNoProblemPacket(int threadCount) {
        supplier = newSupplier(threadCount);
        supplier.start();
        final Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        final long callId = invocation.op.getCallId();

        final Packet response = new Packet(serializationService.toBytes(new NormalResponse("foo", callId, 0, false)))
                .setPacketType(Packet.Type.OPERATION)
                .raiseFlags(FLAG_OP_RESPONSE)
                .setConn(mock(ServerConnection.class));

        supplier.get().accept(response);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                Invocation inv = invocationRegistry.get(callId);
                System.out.println(inv);
                assertNull(inv);
            }
        });

        assertEquals(1, supplier.responsesNormal());
        assertEquals(0, supplier.responsesBackup());
        assertEquals(0, supplier.responsesError());
        assertEquals(0, supplier.responsesMissing());
        assertEquals(0, supplier.responsesTimeout());
        assertEquals(0, supplier.responseQueueSize());
    }

    // test that is a bad response is send, the processing loop isn't broken
    // This test isn't terribly exciting since responses are constructed by
    // the system and unlikely to fail.
    @Test
    public void whenPacketThrowsException() {
        supplier = newSupplier(1);
        supplier.start();

        // create a registered invocation
        final Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);
        final long callId = invocation.op.getCallId();

        // the response flag isn't set; so an exception is thrown.
        Packet badResponse = new Packet(serializationService.toBytes(new NormalResponse("bad", 1, 0, false)))
                .setPacketType(Packet.Type.OPERATION)
                .setConn(mock(ServerConnection.class));

        Consumer<Packet> responseConsumer = supplier.get();

        responseConsumer.accept(badResponse);

        final Packet goodResponse = new Packet(serializationService.toBytes(new NormalResponse("foo", callId, 0, false)))
                .setPacketType(Packet.Type.OPERATION)
                .raiseFlags(FLAG_OP_RESPONSE)
                .setConn(mock(ServerConnection.class));

        responseConsumer.accept(goodResponse);

        assertTrueEventually(() -> {
            Invocation inv = invocationRegistry.get(callId);
            System.out.println(inv);
            assertNull(inv);
        });
    }

    private Invocation newInvocation() {
        Invocation.Context context = new Invocation.Context(
                null, null, null, null, null, 0, invocationRegistry, null, null, null, null, null, null, null, null, null, null, null, null);

        Operation op = new DummyOperation();
        return new PartitionInvocation(context, op, 0, 0, 0, false, false);
    }
}
