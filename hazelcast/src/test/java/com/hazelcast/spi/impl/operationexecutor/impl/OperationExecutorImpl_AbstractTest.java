package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.config.Config;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.spi.properties.GroupProperty.GENERIC_OPERATION_THREAD_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_OPERATION_THREAD_COUNT;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Abstract test support to test the {@link OperationExecutorImpl}.
 * <p/>
 * The idea is the following; all dependencies for the executor are available as fields in this object and by calling the
 * {@link #initExecutor()} method, the actual ClassicOperationExecutor instance is created. But if you need to replace
 * the dependencies by mocks, just replace them before calling the {@link #initExecutor()} method.
 */
public abstract class OperationExecutorImpl_AbstractTest extends HazelcastTestSupport {

    protected LoggingServiceImpl loggingService;
    protected HazelcastProperties props;
    protected Address thisAddress;
    protected HazelcastThreadGroup threadGroup;
    protected DefaultNodeExtension nodeExtension;
    protected OperationRunnerFactory handlerFactory;
    protected InternalSerializationService serializationService;
    protected PacketHandler responsePacketHandler;
    protected OperationExecutorImpl executor;
    protected Config config;

    @Before
    public void setup() throws Exception {
        loggingService = new LoggingServiceImpl("foo", "jdk", new BuildInfo("1", "1", "1", 1, false, (byte) 1));

        serializationService = new DefaultSerializationServiceBuilder().build();
        config = new Config();
        config.setProperty(PARTITION_COUNT.getName(), "10");
        config.setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), "10");
        config.setProperty(GENERIC_OPERATION_THREAD_COUNT.getName(), "10");
        thisAddress = new Address("localhost", 5701);
        threadGroup = new HazelcastThreadGroup(
                "foo",
                loggingService.getLogger(HazelcastThreadGroup.class),
                Thread.currentThread().getContextClassLoader());
        nodeExtension = new DefaultNodeExtension(Mockito.mock(Node.class));
        handlerFactory = new DummyOperationRunnerFactory();

        responsePacketHandler = new DummyResponsePacketHandler();
    }

    protected OperationExecutorImpl initExecutor() {
        props = new HazelcastProperties(config);
        executor = new OperationExecutorImpl(
                props, loggingService, thisAddress, handlerFactory,
                threadGroup, nodeExtension);
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

    protected class DummyResponsePacketHandler implements PacketHandler {
        protected List<Packet> packets = synchronizedList(new LinkedList<Packet>());
        protected List<Response> responses = synchronizedList(new LinkedList<Response>());

        @Override
        public void handle(Packet packet) throws Exception {
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

    protected static class DummyGenericOperation extends DummyOperation {
        public DummyGenericOperation() {
            super(GENERIC_PARTITION_ID);
        }
    }

    protected static class DummyPartitionOperation extends DummyOperation {
        public DummyPartitionOperation() {
            this(0);
        }

        public DummyPartitionOperation(int partitionId) {
            super(partitionId);
        }
    }

    protected static class DummyOperation extends AbstractOperation {
        private int durationMs;

        public DummyOperation(int partitionId) {
            setPartitionId(partitionId);
        }

        public DummyOperation durationMs(int durationMs) {
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

    protected class DummyOperationRunnerFactory implements OperationRunnerFactory {
        protected List<DummyOperationRunner> partitionOperationHandlers = new LinkedList<DummyOperationRunner>();
        protected List<DummyOperationRunner> genericOperationHandlers = new LinkedList<DummyOperationRunner>();
        protected DummyOperationRunner adhocHandler;

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

    public class DummyOperationRunner extends OperationRunner {
        protected List<Packet> packets = synchronizedList(new LinkedList<Packet>());
        protected List<Operation> operations = synchronizedList(new LinkedList<Operation>());
        protected List<Runnable> tasks = synchronizedList(new LinkedList<Runnable>());

        public DummyOperationRunner(int partitionId) {
            super(partitionId);
        }

        @Override
        public void run(Runnable task) {
            tasks.add(task);
            task.run();
        }

        @Override
        public void run(Packet packet) throws Exception {
            packets.add(packet);
            Operation op = serializationService.toObject(packet);
            run(op);
        }

        @Override
        public void run(Operation task) {
            operations.add(task);

            this.currentTask = task;
            try {
                task.run();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                currentTask = null;
            }
        }
    }


    protected static class UrgentDummyOperation extends DummyOperation implements UrgentSystemOperation {

        public UrgentDummyOperation(int partitionId) {
            super(partitionId);
        }
    }

    protected static class DummyOperationHostileThread extends Thread implements OperationHostileThread {
        protected DummyOperationHostileThread(Runnable task) {
            super(task);
        }
    }
}
