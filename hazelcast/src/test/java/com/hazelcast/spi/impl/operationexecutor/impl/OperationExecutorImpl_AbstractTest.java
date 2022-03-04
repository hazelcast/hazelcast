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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.config.Config;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.impl.LoggingServiceImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Abstract test support to test the {@link OperationExecutorImpl}.
 * <p>
 * The idea is the following; all dependencies for the executor are available as fields in this object and by calling the
 * {@link #initExecutor()} method, the actual ClassicOperationExecutor instance is created. But if you need to replace
 * the dependencies by mocks, just replace them before calling the {@link #initExecutor()} method.
 */
public abstract class OperationExecutorImpl_AbstractTest extends HazelcastTestSupport {

    LoggingServiceImpl loggingService;
    HazelcastProperties props;
    Address thisAddress;
    DefaultNodeExtension nodeExtension;
    OperationRunnerFactory handlerFactory;
    InternalSerializationService serializationService;
    Consumer<Packet> responsePacketConsumer;
    OperationExecutorImpl executor;
    Config config;

    @Before
    public void setup() throws Exception {
        loggingService = new LoggingServiceImpl("foo", "jdk", new BuildInfo("1", "1", "1", 1, false, (byte) 1, "1"), true, null);

        serializationService = new DefaultSerializationServiceBuilder().build();
        config = smallInstanceConfig();
        thisAddress = new Address("localhost", 5701);
        Node node = Mockito.mock(Node.class);
        when(node.getConfig()).thenReturn(config);
        when(node.getProperties()).thenReturn(new HazelcastProperties(config));
        when(node.getVersion()).thenReturn(new MemberVersion(0, 0, 0));
        nodeExtension = new DefaultNodeExtension(node);
        handlerFactory = new DummyOperationRunnerFactory();

        responsePacketConsumer = new DummyResponsePacketConsumer();
    }

    protected OperationExecutorImpl initExecutor() {
        props = new HazelcastProperties(config);
        executor = new OperationExecutorImpl(
                props, loggingService, thisAddress, handlerFactory, nodeExtension, "hzName", Thread.currentThread().getContextClassLoader());
        executor.start();
        return executor;
    }

    public static <E> void assertEqualsEventually(final PartitionSpecificCallable task, final E expected) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(task + " has not given a response", task.completed());
                assertEquals(expected, task.getResult());
            }
        });
    }

    class DummyResponsePacketConsumer implements Consumer<Packet> {

        List<Packet> packets = synchronizedList(new LinkedList<Packet>());
        List<Response> responses = synchronizedList(new LinkedList<Response>());

        @Override
        public void accept(Packet packet) {
            packets.add(packet);
            Response response = serializationService.toObject(packet);
            responses.add(response);
        }
    }

    @After
    public void teardown() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    static class DummyGenericOperation extends DummyOperation {

        DummyGenericOperation() {
            super(GENERIC_PARTITION_ID);
        }
    }

    static class DummyPartitionOperation extends DummyOperation {

        DummyPartitionOperation() {
            this(0);
        }

        DummyPartitionOperation(int partitionId) {
            super(partitionId);
        }
    }

    static class DummyOperation extends Operation {

        private int durationMs;

        DummyOperation(int partitionId) {
            setPartitionId(partitionId);
        }

        DummyOperation durationMs(int durationMs) {
            this.durationMs = durationMs;
            return this;
        }

        @Override
        public void run() throws Exception {
            try {
                Thread.sleep(durationMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeInt(durationMs);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            durationMs = in.readInt();
        }
    }

    class DummyOperationRunnerFactory implements OperationRunnerFactory {

        List<DummyOperationRunner> partitionOperationHandlers = new LinkedList<DummyOperationRunner>();
        List<DummyOperationRunner> genericOperationHandlers = new LinkedList<DummyOperationRunner>();
        DummyOperationRunner adhocHandler;

        @Override
        public OperationRunner createPartitionRunner(int partitionId) {
            DummyOperationRunner operationHandler = new DummyOperationRunner(partitionId);
            partitionOperationHandlers.add(operationHandler);
            return operationHandler;
        }

        @Override
        public OperationRunner createGenericRunner() {
            DummyOperationRunner operationHandler = new DummyOperationRunner(Operation.GENERIC_PARTITION_ID);
            genericOperationHandlers.add(operationHandler);
            return operationHandler;
        }

        @Override
        public OperationRunner createAdHocRunner() {
            if (adhocHandler != null) {
                throw new IllegalStateException("adHocHandler should only be created once");
            }
            // not the correct handler because it publishes the operation
            DummyOperationRunner operationHandler = new DummyOperationRunner(-2);
            adhocHandler = operationHandler;
            return operationHandler;
        }
    }

    class DummyOperationRunner extends OperationRunner {

        List<Packet> packets = synchronizedList(new LinkedList<Packet>());
        List<Operation> operations = synchronizedList(new LinkedList<Operation>());
        List<Runnable> tasks = synchronizedList(new LinkedList<Runnable>());

        DummyOperationRunner(int partitionId) {
            super(partitionId);
        }

        @Override
        public long executedOperationsCount() {
            return 0;
        }

        @Override
        public void run(Runnable task) {
            tasks.add(task);
            task.run();
        }

        @Override
        public boolean run(Packet packet) throws Exception {
            packets.add(packet);
            Operation op = serializationService.toObject(packet);
            run(op);
            return false;
        }

        @Override
        public boolean run(Operation task) {
            operations.add(task);

            currentTask = task;
            try {
                task.run();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                currentTask = null;
            }
            return false;
        }
    }

    static class UrgentDummyOperation extends DummyOperation implements UrgentSystemOperation {

        UrgentDummyOperation(int partitionId) {
            super(partitionId);
        }
    }

    static class DummyOperationHostileThread extends Thread implements OperationHostileThread {
        DummyOperationHostileThread(Runnable task) {
            super(task);
        }
    }
}
