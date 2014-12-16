package com.hazelcast.executor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ExecutorTest extends HazelcastTestSupport {

    @Test
    public void submitToAllMembers_WithStatefulCallable() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance();
        factory.newHazelcastInstance();
        IExecutorService executorService = instance1.getExecutorService(randomString());

        MyTask myTask = new MyTask();
        final CountDownLatch completedLatch = new CountDownLatch(1);
        final AtomicBoolean failed = new AtomicBoolean();
        // Local execution of callable may change the state of callable before sent to other members
        // we avoid this by serializing beforehand
        executorService.submitToAllMembers(myTask, new MultiExecutionCallback() {
            @Override
            public void onResponse(Member member, Object value) {
                if ((Integer) value != 1) {
                    failed.set(true);
                }
            }

            @Override
            public void onComplete(Map<Member, Object> values) {
                completedLatch.countDown();
            }
        });
        completedLatch.await(1, TimeUnit.MINUTES);
        assertFalse(failed.get());
    }

    private static class MyTask implements Callable<Integer>, Serializable {
        private int state;

        @Override
        public Integer call() throws Exception {
            return ++state;
        }
    }
}