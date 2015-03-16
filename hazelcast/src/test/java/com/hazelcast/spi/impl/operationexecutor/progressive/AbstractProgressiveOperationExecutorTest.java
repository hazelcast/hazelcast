package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.config.Config;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.NIOThread;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.Response;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;
import com.hazelcast.spi.impl.operationexecutor.ResponsePacketHandler;
import com.hazelcast.spi.impl.operationexecutor.classic.GenericOperationThread;
import com.hazelcast.spi.impl.operationexecutor.classic.PartitionOperationThread;
import com.hazelcast.spi.impl.operationexecutor.classic.PartitionSpecificCallable;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Abstract test support to test the {@link AbstractProgressiveOperationExecutorTest}.
 * <p/>
 * The idea is the following; all dependencies for the scheduler are available as fields in this object and by calling the
 * {@link #initExecutor()} method, the actual ClassicOperationScheduler instance is created. But if you need to replace
 * the dependencies by mocks, just replace them before calling the {@link #initExecutor()} method.
 */
public abstract class AbstractProgressiveOperationExecutorTest extends HazelcastTestSupport {

    protected LoggingServiceImpl loggingService;
    protected GroupProperties groupProperties;
    protected Address thisAddress;
    protected HazelcastThreadGroup threadGroup;
    protected DefaultNodeExtension nodeExtension;
    protected OperationRunnerFactory runnerFactory;
    protected SerializationService serializationService;
    protected ResponsePacketHandler responsePacketHandler;
    protected ProgressiveOperationExecutor executor;
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
        runnerFactory = new DummyOperationRunnerFactory();

        responsePacketHandler = new DummyResponsePacketHandler();
    }

    protected ProgressiveOperationExecutor initExecutor() {
        groupProperties = new GroupProperties(config);
        executor = new ProgressiveOperationExecutor(
                groupProperties, loggingService, runnerFactory, nodeExtension, responsePacketHandler,
                threadGroup);
        executor.start();
        return executor;
    }

    public static <E> void assertCompletesEventually(final PartitionSpecificCallable task, final E expected) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(task + " has not given a response", task.completed());
                assertEquals(expected, task.getResult());
            }
        });
    }

    public static void awaitCompletion(DummyOperation... operations) throws Throwable {
        long timeoutMs = TimeUnit.MINUTES.toMicros(ASSERT_TRUE_EVENTUALLY_TIMEOUT);
        for (DummyOperation op : operations) {
            long startMs = System.currentTimeMillis();
            if (startMs < 0) {
                throw new GetTimeoutException("No timeout left to call get on operation:"+op);
            }
            op.get(timeoutMs);
            long durationMs = System.currentTimeMillis() - startMs;
            timeoutMs -= durationMs;
        }
    }

    public static void assertExecutedByPartitionThread(DummyOperation op) {
        Assert.assertNotNull(op.getExecutingThread());
        assertInstanceOf(PartitionOperationThread.class, op.getExecutingThread());
    }

    public static void assertExecutedByGenericThread(DummyOperation op) {
        Assert.assertNotNull(op.getExecutingThread());
        assertInstanceOf(GenericOperationThread.class, op.getExecutingThread());
    }

    public static void assertExecutedByCurrentThread(DummyOperation... ops) {
        for (DummyOperation op : ops) {
            Assert.assertNotNull(op.getExecutingThread());
            assertSame(Thread.currentThread(), op.getExecutingThread());
        }
    }

    public static void assertExecutedBySameThread(DummyOperation op1, DummyOperation op2) {
        Assert.assertNotNull(op1.getExecutingThread());
        Assert.assertNotNull(op2.getExecutingThread());
        assertSame(op1.getExecutingThread(), op2.getExecutingThread());
    }

    public static void assertExecutedByDifferentThreads(DummyOperation op1, DummyOperation op2) {
        Assert.assertNotNull(op1.getExecutingThread());
        Assert.assertNotNull(op2.getExecutingThread());
        assertNotSame(op2.getExecutingThread(), op1.getExecutingThread());
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

    protected class DummyOperationRunnerFactory implements OperationRunnerFactory {
        protected List<DummyOperationRunner> partitionRunners = new LinkedList<DummyOperationRunner>();
        protected List<DummyOperationRunner> genericRunners = new LinkedList<DummyOperationRunner>();
        protected DummyOperationRunner adHocRunner;

        @Override
        public OperationRunner createPartitionRunner(int partitionId) {
            DummyOperationRunner runner = new DummyOperationRunner(partitionId);
            partitionRunners.add(runner);
            return runner;
        }

        @Override
        public OperationRunner createGenericRunner() {
            DummyOperationRunner runner = new DummyOperationRunner(-1);
            genericRunners.add(runner);
            return runner;
        }

        @Override
        public OperationRunner createAdHocRunner() {
            if (adHocRunner != null) {
                throw new IllegalStateException("adHocHandler should only be created once");
            }
            // not the correct handler because it publishes the operation
            DummyOperationRunner runner = new DummyOperationRunner(-1);
            adHocRunner = runner;
            return runner;
        }
    }

    public class DummyOperationRunner extends OperationRunner {
        protected List<Packet> packets = synchronizedList(new LinkedList<Packet>());
        protected List<Operation> operations = synchronizedList(new LinkedList<Operation>());
        protected List<Runnable> tasks = synchronizedList(new LinkedList<Runnable>());
        protected Map<Packet, Operation> packetToOperationMap = new ConcurrentHashMap<Packet, Operation>();

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
            packetToOperationMap.put(packet, op);
            run(op);
        }

        public Operation getOperation(Packet packet) {
            return packetToOperationMap.get(packet);
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
