package com.hazelcast.concurrent.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;

public class SemaphoreWaitTest extends HazelcastTestSupport{

    private HazelcastInstance hz;
    private ISemaphore semaphore;

    @Before
    public void setup(){
        setLoggingLog4j();
        Config config= new Config();
        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS, "5000");
        hz = createHazelcastInstance(config);
        semaphore = hz.getSemaphore("s");
        semaphore.init(0);
    }

    @Test
    public void longWaitingAndEventualTimeout() throws InterruptedException {
        long startMs = System.currentTimeMillis();
        boolean result = semaphore.tryAcquire(10, TimeUnit.SECONDS);
        long durationMs = System.currentTimeMillis()-startMs;

        System.out.println("durationMs:"+durationMs);
        assertFalse(result);
    }
}
