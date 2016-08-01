package com.hazelcast.durableexecutor;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DurableLongRunningTaskTest extends HazelcastTestSupport {

    private static final int CALL_TIMEOUT = 2000;

    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = new Config().setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + CALL_TIMEOUT);
        hz = createHazelcastInstance(config);
    }

    @Test
    public void test() {
        final String response = "foobar";
        SleepingCallable task = new SleepingCallable(response, 10 * CALL_TIMEOUT);
        final Future<String> f = hz.getDurableExecutorService("e").submit(task);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(f.isDone());
                assertEquals(response, f.get());
            }
        });
    }

    public static class SleepingCallable implements Callable<String>, Serializable, HazelcastInstanceAware {

        private final String response;
        private final int delayMs;
        private ILogger logger;

        SleepingCallable(String response, int delayMs) {
            this.response = response;
            this.delayMs = delayMs;
        }

        @Override
        public String call() throws Exception {
            logger.info("SleepingCallable task started");
            Thread.sleep(delayMs);
            return response;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            logger = instance.getLoggingService().getLogger("DurableLongRunningTaskTest");
        }
    }
}
