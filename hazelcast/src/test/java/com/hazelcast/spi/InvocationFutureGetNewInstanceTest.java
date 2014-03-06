package com.hazelcast.spi;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * This test verifies that instances returned by the InvocationFuture, are always copied instances.
 * We don't want instances to be shared unexpectedly. In the future there might be some sharing going
 * on when we see that an instance is immutable.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class InvocationFutureGetNewInstanceTest extends HazelcastTestSupport {

    private static HazelcastInstance[] instances;
    private static HazelcastInstance local;
    private static HazelcastInstance remote;

    @BeforeClass
    public static void setUp() {
        instances = new TestHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(instances);
        local = instances[0];
        remote = instances[1];
    }

    @Test
    public void invocationToLocalMember() throws ExecutionException, InterruptedException {
        Node localNode = getNode(local);

        Object response = localNode.nodeEngine.toData(new DummyObject());
        Operation op = new OperationWithResponse(response);

        OperationService service = localNode.nodeEngine.getOperationService();
        Future f = service.createInvocationBuilder(null, op, localNode.address).invoke();
        Object instance1 = f.get();
        Object instance2 = f.get();

        assertNotNull(instance1);
        assertNotNull(instance2);
        assertTrue(instance1 instanceof DummyObject);
        assertTrue(instance2 instanceof DummyObject);
        assertNotSame(instance1, instance2);
        assertNotSame(instance1, response);
        assertNotSame(instance2, response);
    }

    @Test
    public void invocationToRemoteMember() throws ExecutionException, InterruptedException {
        Node localNode = getNode(local);

        Object response = localNode.nodeEngine.toData(new DummyObject());
        Operation op = new OperationWithResponse(response);

        Address remoteAddress = getNode(remote).address;

        OperationService operationService = localNode.nodeEngine.getOperationService();
        Future f = operationService.createInvocationBuilder(null, op, remoteAddress).invoke();
        Object instance1 = f.get();
        Object instance2 = f.get();

        assertNotNull(instance1);
        assertNotNull(instance2);
        assertTrue(instance1 instanceof DummyObject);
        assertTrue(instance2 instanceof DummyObject);
        assertNotSame(instance1, instance2);
        assertNotSame(instance1, response);
        assertNotSame(instance2, response);
    }

    public static class DummyObject implements Serializable {
    }

    public static class OperationWithResponse extends AbstractOperation {
        private Object response;

        public OperationWithResponse() {
        }

        public OperationWithResponse(Object response) {
            this.response = response;
        }

        @Override
        public void run() throws Exception {
            //no-op
        }

        @Override
        public boolean returnsResponse() {
            return true;
        }

        @Override
        public Object getResponse() {
            return response;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeObject(response);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            response = in.readObject();
        }
    }

}
