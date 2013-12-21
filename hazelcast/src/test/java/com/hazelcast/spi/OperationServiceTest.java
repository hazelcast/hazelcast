package com.hazelcast.spi;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionView;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class OperationServiceTest extends HazelcastTestSupport {

    @Test
    public void test() throws InterruptedException {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();

        System.out.println("Waiting");
        waitForPartitionStabilization();
        System.out.println("Completed waiting");
    }

    @Test
    public void invokeOnRemoteTarget() throws Exception {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance remote = instances[1];

        DummyOperation op = new DummyOperation();
        OperationService operationService = getNode(local).nodeEngine.getOperationService();
        Future f = operationService.invokeOnTarget(null, op, new Address(remote.getCluster().getLocalMember().getSocketAddress()));
        assertEquals(new Integer(10), f.get());
    }

    @Test
    public void invokeOnLocalTarget() throws Exception {
        HazelcastInstance local = createHazelcastInstanceFactory(1).newHazelcastInstance();

        DummyOperation op = new DummyOperation();
        OperationService operationService = getNode(local).nodeEngine.getOperationService();
        Future f = operationService.invokeOnTarget(null, op, new Address(local.getCluster().getLocalMember().getSocketAddress()));
        assertEquals(new Integer(10), f.get());
    }

    @Test
    public void invokeOnLocalPartition()throws Exception{
        HazelcastInstance local = createHazelcastInstanceFactory(1).newHazelcastInstance();

        DummyOperation op = new DummyOperation();
        OperationService operationService = getNode(local).nodeEngine.getOperationService();
        Future f = operationService.invokeOnPartition(null, op, 1);
        assertEquals(new Integer(10), f.get());
    }

    @Test
    public void invokeOnRemotePartition()throws Exception{
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance remote = instances[1];

        waitForPartitionStabilization();

        DummyOperation op = new DummyOperation();
        OperationService operationService = getNode(local).nodeEngine.getOperationService();
        int partitionId = findAnyAssignedPartition(remote);
        Future f = operationService.invokeOnPartition(null, op, partitionId);
        assertEquals(new Integer(10), f.get());
    }

    private int findAnyAssignedPartition(HazelcastInstance hz){
        for(Partition p: hz.getPartitionService().getPartitions()){
           if(p.getOwner().localMember()){
               return p.getPartitionId();
           }
        }

        throw new RuntimeException("No owned partition found");
    }

    private void waitForPartitionStabilization() throws InterruptedException {
        System.out.println("Starting wait for partition stabilization");
        Thread.sleep(5000);
        System.out.println("Finished wait for partition stabilization");
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
