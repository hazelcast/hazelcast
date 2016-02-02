package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class Invocation_InterruptTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private InternalOperationService opService;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        opService = getOperationService(hz);
    }

    @Test
    public void whenOperationNotInterruptable() throws Exception {
        DummyPartitionOperation op = new DummyPartitionOperation() {
            public void run() {
                try {
                    Thread.sleep(2000);
                    result = false;
                } catch (InterruptedException e) {
                    result = true;
                }
            }
        };

        InvocationFuture f = (InvocationFuture) opService.invokeOnPartition(null, op, op.getPartitionId());
        assertEquals(Boolean.FALSE, f.get());
    }
}
