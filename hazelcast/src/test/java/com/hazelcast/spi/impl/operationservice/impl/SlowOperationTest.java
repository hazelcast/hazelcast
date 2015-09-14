package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastTestSupport;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.fail;

public class SlowOperationTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private InternalOperationService operationService;
    private HazelcastInstance[] cluster;

    @Before
    public void setup() {
        setLoggingLog4j();
        setLogLevel(Level.DEBUG);
        Config config = new Config();
        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS, "10000");

        cluster = createHazelcastInstanceFactory(2).newInstances(config);
        hz = cluster[0];
        operationService = getOperationService(hz);
    }

//    @Test
//    public void test() throws InterruptedException {
//
//        ISemaphore s = hz.getSemaphore(generateKeyOwnedBy(cluster[1]));
//        s.init(1);
//
//        System.out.println("Acquiring first permit");
//        s.acquire();
//        System.out.println("Acquiring second permit");
//        s.acquire();
//        System.out.println("Acquired");
//    }

    @Test(expected = OperationTimeoutException.class)
    public void testSlowOperation() throws ExecutionException, InterruptedException {
        Operation op = new SlowOperation();
        InternalCompletableFuture f = operationService.invokeOnPartition(null, op, getPartitionId(cluster[1]));
        f.getSafely();
    }

//    @Test
//    public void testExecutor() throws ExecutionException, InterruptedException {
//        IExecutorService executorService = hz.getExecutorService("foo");
//        Future f = executorService.submitToMember(new SlowCallable(), cluster[1].getCluster().getLocalMember());
//        try {
//            f.get();
//            fail();
//        }catch (ExecutionException e){
//            assertInstanceOf(OperationTimeoutException.class, e.getCause());
//        }
//    }

    public static class SlowCallable implements Callable,Serializable {
        @Override
        public Object call() {
            System.out.println("start sleeping----------------------------");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("completed sleeping-------------------------");
            return null;
        }
    }

    public static class SlowOperation extends AbstractOperation {
        @Override
        public void run() throws Exception {
            System.out.println("start sleeping");
            Thread.sleep(10000);
            System.out.println("completed sleeping");
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }
    }

}
