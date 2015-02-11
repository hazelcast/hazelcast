package com.hazelcast.spi.impl.classicscheduler;

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
import com.hazelcast.spi.impl.ResponsePacketHandler;
import com.hazelcast.spi.impl.OperationHandler;
import com.hazelcast.spi.impl.OperationHandlerFactory;
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
 * Abstract test support to test the {@link com.hazelcast.spi.impl.classicscheduler.AbstractClassicSchedulerTest}.
 * <p/>
 * The idea is the following; all dependencies for the scheduler are available as fields in this object and by calling the
 * {@link #initScheduler()} method, the actual ClassicOperationScheduler instance is created. But if you need to replace
 * the dependencies by mocks, just replace them before calling the {@link #initScheduler()} method.
 */
public abstract class AbstractClassicSchedulerTest extends HazelcastTestSupport {

    protected LoggingServiceImpl loggingService;
    protected GroupProperties groupProperties;
    protected Address thisAddress;
    protected HazelcastThreadGroup threadGroup;
    protected DefaultNodeExtension nodeExtension;
    protected OperationHandlerFactory handlerFactory;
    protected SerializationService serializationService;
    protected ResponsePacketHandler responsePacketHandler;
    protected ClassicOperationScheduler scheduler;
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
        handlerFactory = new DummyOperationHandlerFactory();

        responsePacketHandler = new DummyResponsePacketHandler();
    }

    protected ClassicOperationScheduler initScheduler() {
        groupProperties = new GroupProperties(config);
        scheduler = new ClassicOperationScheduler(
                groupProperties, loggingService, thisAddress, handlerFactory, responsePacketHandler,
                threadGroup, nodeExtension);
        return scheduler;
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
        if (scheduler != null) {
            scheduler.shutdown();
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

    protected class DummyOperationHandlerFactory implements OperationHandlerFactory {
        protected List<DummyOperationHandler> partitionOperationHandlers = new LinkedList<DummyOperationHandler>();
        protected List<DummyOperationHandler> genericOperationHandlers = new LinkedList<DummyOperationHandler>();
        protected DummyOperationHandler adhocHandler;

        @Override
        public OperationHandler createPartitionHandler(int partitionId) {
            DummyOperationHandler operationHandler = new DummyOperationHandler();
            partitionOperationHandlers.add(operationHandler);
            return operationHandler;
        }

        @Override
        public OperationHandler createGenericOperationHandler() {
            DummyOperationHandler operationHandler = new DummyOperationHandler();
            genericOperationHandlers.add(operationHandler);
            return operationHandler;
        }

        @Override
        public OperationHandler createAdHocOperationHandler() {
            if (adhocHandler != null) {
                throw new IllegalStateException("adHocHandler should only be created once");
            }
            // not the correct handler because it publishes the operation
            DummyOperationHandler operationHandler = new DummyOperationHandler();
            adhocHandler = operationHandler;
            return operationHandler;
        }
    }

    public class DummyOperationHandler extends OperationHandler {
        protected List<Packet> packets = synchronizedList(new LinkedList<Packet>());
        protected List<Operation> operations = synchronizedList(new LinkedList<Operation>());
        protected List<Runnable> tasks = synchronizedList(new LinkedList<Runnable>());

        @Override
        public void process(Runnable task) {
            tasks.add(task);
            task.run();
        }

        @Override
        public void process(Packet packet) throws Exception {
            packets.add(packet);
            Operation op = serializationService.toObject(packet.getData());
            process(op);
        }

        @Override
        public void process(Operation task) {
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
