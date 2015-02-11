package com.hazelcast.spi;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class InvocationRetryTest extends HazelcastTestSupport {

    @Test
    public void whenPartitionTargetMemberDiesThenOperationSendToNewPartitionOwner() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance();
        HazelcastInstance remote = factory.newHazelcastInstance();
        warmUpPartitions(local, remote);

        Node localNode = getNode(local);
        OperationService service = localNode.nodeEngine.getOperationService();
        Operation op = new PartitionTargetOperation();
        Future f = service.createInvocationBuilder(null, op, getPartitionId(remote))
                .setCallTimeout(30000)
                .invoke();
        sleepSeconds(1);

        remote.shutdown();

        //the get should work without a problem because the operation should be re-targeted at the newest owner
        //for that given partition
        f.get();
    }

    @Test
    public void whenTargetMemberDiesThenOperationAbortedWithMembersLeftException() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = factory.newHazelcastInstance();
        HazelcastInstance remote = factory.newHazelcastInstance();
        warmUpPartitions(local, remote);

        OperationService service = getNode(local).nodeEngine.getOperationService();
        Operation op = new TargetOperation();
        Address address = new Address(remote.getCluster().getLocalMember().getSocketAddress());
        Future f = service.createInvocationBuilder(null, op, address).invoke();
        sleepSeconds(1);

        remote.getLifecycleService().terminate();

        try {
            f.get();
            fail();
        } catch (MemberLeftException expected) {

        }
    }

    /**
     * Operation send to a specific member.
     */
    private static class TargetOperation extends AbstractOperation {
        public void run() throws InterruptedException {
            Thread.sleep(10000);
        }
    }

    /**
     * Operation send to a specific target partition.
     */
    private static class PartitionTargetOperation extends AbstractOperation implements PartitionAwareOperation {

        public void run() throws InterruptedException {
            Thread.sleep(5000);
        }
    }
}
