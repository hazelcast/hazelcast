package com.hazelcast.spi.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BasicOperationService_invokeOnTargetTest extends HazelcastTestSupport {

    @Test
    public void test_whenSelf() {
        HazelcastInstance[] nodes = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance localNode = nodes[0];
        OperationService operationService = getOperationService(localNode);
        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected);
        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, getAddress(localNode));
        assertEquals(expected, invocation.getSafely());

        //todo: we need to verify that the call was run on the calling thread
    }

    @Test
    public void test_whenRemote() {
        HazelcastInstance[] nodes = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance localNode = nodes[0];
        HazelcastInstance remoteNode = nodes[1];
        OperationService operationService = getOperationService(localNode);
        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected);
        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, getAddress(remoteNode));
        assertEquals(expected, invocation.getSafely());
    }

    //this test is very slow, so it is marked as a nightly test
    @Test
    @Category(NightlyTest.class)
    public void test_nonExistingTarget() throws UnknownHostException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS, "10000");
        HazelcastInstance[] nodes = createHazelcastInstanceFactory(2).newInstances(config);
        HazelcastInstance localNode = nodes[0];
        HazelcastInstance remoteNode = nodes[1];
        Address remoteAddress = new Address("127.0.0.1",1);//getAddress(remoteNode);
        remoteNode.shutdown();

        OperationService operationService = getOperationService(localNode);
        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected);
        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, remoteAddress);

        try {
            invocation.getSafely();
            fail();
        }catch (TargetNotMemberException e) {
        }
    }

    @Test
    public void test_whenExceptionThrownInOperationRun() {

    }

    public static class DummyOperation extends AbstractOperation {
        private Object value;

        public DummyOperation() {
        }

        public DummyOperation(Object value) {
            this.value = value;
        }

        @Override
        public void run() throws Exception {

        }

        @Override
        public Object getResponse() {
            return value;
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeObject(value);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            value = in.readObject();
        }
    }
}
