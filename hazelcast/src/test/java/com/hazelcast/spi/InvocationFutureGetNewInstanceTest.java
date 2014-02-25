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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

/**
 * This test verifies that instances returned by the InvocationFuture, are always copied instances.
 * We don't want instances to be shared unexpectedly. In the future there might be some sharing going
 * on when we see that an instance is immutable.
 */
//TODO: This test is currently disabled, because we are violating the assumption that a user will always
//get a copy of the object.
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class InvocationFutureGetNewInstanceTest extends HazelcastTestSupport {

    @Test
    @Ignore
    public void invocationToLocalMember() throws ExecutionException, InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance();
        warmUpPartitions(local);

        Node localNode = getNode(local);

        DummyObject dummyObject = new DummyObject();
        Operation op = new OperationWithResponse(dummyObject);

        OperationService service = localNode.nodeEngine.getOperationService();
        Future f = service.createInvocationBuilder(null, op, localNode.address).invoke();
        Object instance1 = f.get();
        Object instance2 = f.get();

        assertNotNull(instance1);
        assertNotNull(instance2);
        assertNotSame(instance1, instance2);
        assertNotSame(instance1, dummyObject);
        assertNotSame(instance2, dummyObject);
    }

    @Test
    @Ignore
    public void invocationToRemoteMember() throws ExecutionException, InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance();
        HazelcastInstance remote = factory.newHazelcastInstance();
        warmUpPartitions(local, remote);

        Node localNode = getNode(local);

        DummyObject dummyObject = new DummyObject();
        Operation op = new OperationWithResponse(dummyObject);

        Address remoteAddress = getNode(remote).address;

        OperationService operationService = localNode.nodeEngine.getOperationService();
        Future f = operationService.createInvocationBuilder(null, op, remoteAddress).invoke();
        Object instance1 = f.get();
        Object instance2 = f.get();

        assertNotNull(instance1);
        assertNotNull(instance2);
        assertNotSame(instance1, instance2);
        assertNotSame(instance1, dummyObject);
        assertNotSame(instance2, dummyObject);
    }

    public static class DummyObject implements Serializable {
    }

    public static class OperationWithResponse extends AbstractOperation {
        private Object response;

        public OperationWithResponse() {
        }

        public OperationWithResponse(DummyObject response) {
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
