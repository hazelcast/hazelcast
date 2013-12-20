package com.hazelcast.spi;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class ClusterTest extends HazelcastTestSupport {

    @Test
    public void test() throws InterruptedException {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();

        System.out.println("Waiting");
        Thread.sleep(10000);
        System.out.println("Completed waiting");
    }

    @Test
    public void invokeOnRemoteTargetTest() throws Exception {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance remote = instances[1];

        DummyOperation op = new DummyOperation();
        OperationService operationService = getNode(local).nodeEngine.getOperationService();
        Future f = operationService.invokeOnTarget(null, op, new Address(remote.getCluster().getLocalMember().getSocketAddress()));
        assertEquals(new Integer(10), f.get());
    }

    @Test
    public void invokeOnLocalTargetTest() throws Exception {
        HazelcastInstance local = createHazelcastInstanceFactory(1).newHazelcastInstance();

        DummyOperation op = new DummyOperation();
        OperationService operationService = getNode(local).nodeEngine.getOperationService();
        Future f = operationService.invokeOnTarget(null, op, new Address(local.getCluster().getLocalMember().getSocketAddress()));
        assertEquals(new Integer(10), f.get());
    }

    public static class DummyOperation extends AbstractOperation {
        @Override
        public void run() throws Exception {
        }

        @Override
        public boolean returnsResponse() {
            return true;
        }

        @Override
        public Object getResponse() {
            return new Integer(10);
        }

    }
}
