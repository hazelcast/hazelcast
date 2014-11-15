package com.hazelcast.spi;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

public class InvokeOnTargetTest extends HazelcastTestSupport {

    @Test
    public void invokeLocal() throws Exception {
        invoke(true);
    }

    @Test
    public void invokeRemote() throws Exception {
        invoke(false);
    }

    public void invoke(boolean localCall) throws Exception {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance remote = instances[1];

        Node node = getNode(local);
        OperationService operationService = node.nodeEngine.getOperationService();

        SomeOperation op = new SomeOperation();

        Address address;
        if (localCall) {
            address = getNode(local).getThisAddress();
        } else {
            address = getNode(remote).getThisAddress();
        }
        Future f = operationService.invokeOnTarget("foo", op, address);
        Object found = f.get();

        if (localCall) {
            assertEquals(getNode(local).getThisAddress(), found);
        } else {
            assertEquals(getNode(remote).getThisAddress(), found);
        }
    }

    @Test(expected = TimeoutException.class)
    public void invokeOnNoneExisting()throws Exception{
        HazelcastInstance[] instances = createHazelcastInstanceFactory(1).newInstances();
        HazelcastInstance local = instances[0];

        Node node = getNode(local);
        OperationService operationService = node.nodeEngine.getOperationService();

        SomeOperation op = new SomeOperation();

        Address address = new Address("localhost",9000);
        Future f = operationService.invokeOnTarget("foo", op, address);
        f.get(1, TimeUnit.SECONDS);
    }

    public static class SomeOperation extends AbstractOperation {
        private String response;

        public SomeOperation() {
        }

        @Override
        public void run() throws Exception {
        }

        @Override
        public Object getResponse() {
            return getNodeEngine().getThisAddress();
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeUTF(response);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            response = in.readUTF();
        }
    }
}
