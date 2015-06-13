package com.hazelcast.executor.impl;

import com.hazelcast.core.Member;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

// Reproduces issue: https://github.com/hazelcast/hazelcast/issues/5490
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ExecutionCallbackAdapterRaceTest extends HazelcastTestSupport {

    private ILogger logger = Logger.getLogger(ExecutionCallbackAdapterFactory.class);
    private final Member member1 = mock(Member.class);
    private final Member member2 = mock(Member.class);
    private volatile boolean completed;
    private volatile boolean raceDetected;

    @Test
    public void test() throws Exception {
        MultiExecutionCallback callback = new MultiExecutionCallbackMock();

        final ExecutionCallbackAdapterFactory factory = new ExecutionCallbackAdapterFactory(
                logger, asList(member1, member2), callback);

        // first we spawn the response for the member1. This thread is going wait for 2 seconds
        // in the onResponse to trigger the out of order behavior.
        Future f1 = spawn(new Runnable() {
            @Override
            public void run() {
                factory.callbackFor(member1).onResponse("1");
            }
        });

        factory.callbackFor(member2).onResponse("2");

        f1.get();

        assertFalse("Race was detected", raceDetected);
    }

    private class MultiExecutionCallbackMock implements MultiExecutionCallback {
        @Override
        public void onResponse(Member member, Object value) {
            if (member == member1) {
                sleepSeconds(2);
            }

            if (completed) {
                raceDetected = true;
            }
        }

        @Override
        public void onComplete(Map<Member, Object> values) {
            completed = true;
        }
    }
}
