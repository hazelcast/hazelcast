package com.hazelcast.executor;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
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
import java.util.concurrent.TimeoutException;

import static com.hazelcast.instance.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LongRunningTaskTest extends HazelcastTestSupport {

    private static final int CALL_TIMEOUT_MS = 3000;
    private static final int RESULT = 12345;

    private HazelcastInstance hz;
    private IExecutorService executor;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS, "" + CALL_TIMEOUT_MS);

        hz = createHazelcastInstance(config);
        executor = hz.getExecutorService("foo");
    }

    @Test
    public void get() throws Exception {
        Future<Integer> f = executor.submit(new SleepTask(CALL_TIMEOUT_MS * 2, RESULT));
        int result = f.get();

        assertEquals(RESULT, result);
    }

    @Test
    public void getWithTimeout_whenNoTimeout() throws Exception {
        Future<Integer> f = executor.submit(new SleepTask(CALL_TIMEOUT_MS * 2, RESULT));
        int result = f.get(3 * CALL_TIMEOUT_MS, MILLISECONDS);

        assertEquals(RESULT, result);
    }

    @Test(expected = TimeoutException.class)
    public void getWithTimeout_whenTimeout() throws Exception {
        Future<Integer> f = executor.submit(new SleepTask(CALL_TIMEOUT_MS * 2, RESULT));

        f.get(CALL_TIMEOUT_MS, MILLISECONDS);
    }

    static class SleepTask implements Callable<Integer>, Serializable {
        private final int durationMs;
        private final int result;

        public SleepTask(int durationMs, int result) {
            this.durationMs = durationMs;
            this.result = result;
        }

        @Override
        public Integer call() throws Exception {
            Thread.sleep(durationMs);
            return result;
        }
    }
}
