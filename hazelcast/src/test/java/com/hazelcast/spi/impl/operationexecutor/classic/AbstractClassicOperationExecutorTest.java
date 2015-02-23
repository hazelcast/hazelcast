package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.config.Config;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.NIOThread;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.ResponsePacketHandler;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;
import com.hazelcast.spi.impl.Response;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Abstract test support to test the {@link ClassicOperationExecutor}.
 * <p/>
 * The idea is the following; all dependencies for the executor are available as fields in this object and by calling the
 * {@link #initExecutor()} method, the actual ClassicOperationExecutor instance is created. But if you need to replace
 * the dependencies by mocks, just replace them before calling the {@link #initExecutor()} method.
 */
public abstract class AbstractClassicOperationExecutorTest extends HazelcastTestSupport {

    protected LoggingServiceImpl loggingService;
    protected GroupProperties groupProperties;
    protected Address thisAddress;
    protected HazelcastThreadGroup threadGroup;
    protected DefaultNodeExtension nodeExtension;
    protected OperationRunnerFactory handlerFactory;
    protected SerializationService serializationService;
    protected ResponsePacketHandler responsePacketHandler;
    protected ClassicOperationExecutor executor;
    protected Config config;

    @Before
    public void setup() throws Exception {
        loggingService = new LoggingServiceImpl("foo", "jdk", new BuildInfo("1", "1", "1", 1, false));

        serializationService = new DefaultSerializationServiceBuilder().build();
        config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, "10");
        config.setProperty(GroupProperties.PROP_PARTITION_OPERATION_THREAD_COUNT, "10");
        config.setProperty(GroupProperties.PROP_GENERIC_OPERATION_THREAD_COUNT, "10");
        thisAddress = new Address("localhost", 5701);
        threadGroup = new HazelcastThreadGroup(
                "foo",
                loggingService.getLogger(HazelcastThreadGroup.class),
                Thread.currentThread().getContextClassLoader());
        nodeExtension = new DefaultNodeExtension();
        handlerFactory = new DummyOperationRunnerFactory();

        responsePacketHandler = new DummyResponsePacketHandler();
    }

    protected ClassicOperationExecutor initExecutor() {
        groupProperties = new GroupProperties(config);
        executor = new ClassicOperationExecutor(
                groupProperties, loggingService, thisAddress, handlerFactory, responsePacketHandler,
                threadGroup, nodeExtension);
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

    protected class DummyResponsePacketHandler implements ResponsePacketHandler {
        protected List<Packet> packets = synchronizedList(new LinkedList<Packet>());
        protected List<Response> responses = synchronizedList(new LinkedList<Response>());

        @Override
        public Response deserialize(Packet packet) throws Exception {
            packets.add(packet);
            return serializationService.toObject(packet.getData());
        }

        @Override
        public void process(Response task) throws Exception {
            responses.add(task);
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
            super(-1);
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

        public DummyOperation() {
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
            DummyOperationRunner operationHandler = new DummyOperationRunner(-1);
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
            Operation op = serializationService.toObject(packet.getData());
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

    protected static class DummyNioThread extends Thread implements NIOThread {
        protected DummyNioThread(Runnable task) {
            super(task);
        }
    }
}
