package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class PartitionServiceStackOverflowTest extends HazelcastTestSupport {

    @Test
    public void test(){
        HazelcastInstance hz = createHazelcastInstance();
        OperationService opService = getNode(hz).nodeEngine.getOperationService();

        final int iterations = 2000;
        final CountDownLatch latch = new CountDownLatch(iterations);

        for(int k=0;k<iterations;k++){
            opService.executeOperation(new SlowSystemOperation(latch));
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, latch.getCount());
            }
        });
    }

    public static class SlowSystemOperation extends AbstractOperation
            implements UrgentSystemOperation, PartitionAwareOperation{

        private final CountDownLatch latch;

        public SlowSystemOperation(CountDownLatch completedLatch){
            setPartitionId(0);
            this.latch = completedLatch;
        }

        @Override
        public void run() throws Exception {
            Thread.sleep(10);
            latch.countDown();
        }
    }

}
