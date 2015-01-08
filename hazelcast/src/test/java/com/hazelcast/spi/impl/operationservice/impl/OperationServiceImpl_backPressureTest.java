package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class OperationServiceImpl_backPressureTest extends HazelcastTestSupport {
    private HazelcastInstance local;
    private HazelcastInstance remote;
    private OperationServiceImpl localOperationService;

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty(GroupProperties.PROP_BACKPRESSURE_ENABLED, "true")
                .setProperty(GroupProperties.PROP_BACKPRESSURE_SYNCWINDOW, "1");
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);
        local = cluster[0];
        remote = cluster[1];

        localOperationService = (OperationServiceImpl) getOperationService(local);
    }

    @Test
    public void operationWithoutResponse_remoteGenericCall() {
        NoResponseOperation op = new NoResponseOperation();
        InvocationFuture f = (InvocationFuture) localOperationService.invokeOnTarget(null, op, getAddress(remote));
        assertCompletesEventually(f);
    }

    @Test
    public void operationWithoutResponse_localGenericCall() {
        NoResponseOperation op = new NoResponseOperation();
        InvocationFuture f = (InvocationFuture) localOperationService.invokeOnTarget(null, op, getAddress(local));
        assertCompletesEventually(f);
    }

    @Test
    public void operationWithoutResponse_remotePartitionSpecificCall() {
        NoResponseOperation op = new NoResponseOperation();
        InvocationFuture f = (InvocationFuture) localOperationService.invokeOnPartition(null, op, getPartitionId(remote));
        assertCompletesEventually(f);
    }

    @Test
    public void operationWithoutResponse_localPartitionSpecificCall() {
        NoResponseOperation op = new NoResponseOperation();
        InvocationFuture f = (InvocationFuture) localOperationService.invokeOnPartition(null, op, getPartitionId(local));
        assertCompletesEventually(f);
    }

    private void assertCompletesEventually(final InvocationFuture f) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
//                boolean done = f.isDone();
//                assertTrue("the future didn't complete in time", done);

                f.waitForFakeResponse(60, TimeUnit.SECONDS);
                //assertEquals("the expected fake result is not Boolean.FALSE", Boolean.FALSE, result);
            }
        });
    }

    public static class NoResponseOperation extends AbstractOperation {
        public NoResponseOperation() {
        }


        @Override
        public void run() throws Exception {
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }
    }
}
