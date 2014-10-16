package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.client.ClientTestSupport;
import com.hazelcast.client.SimpleClient;
import com.hazelcast.concurrent.countdownlatch.client.AwaitRequest;
import com.hazelcast.concurrent.countdownlatch.client.CountDownRequest;
import com.hazelcast.config.Config;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class CountDownLatchRequestTest extends ClientTestSupport {

    static final String name = randomString();
    static final long timeout = 1000;

    @Override
    protected Config createConfig() {
        return new Config();
    }

    private ICountDownLatch getCountDownLatch() {
        return getInstance().getCountDownLatch(name);
    }

    @Test
    public void testAwait() throws IOException {
        final SimpleClient client = getClient();
        client.send(new AwaitRequest(name, timeout));
        boolean result = (Boolean) client.receive();

        assertTrue(result);
    }

    @Test
    public void testCountDown() throws IOException {
        ICountDownLatch countDownLatch = getCountDownLatch();
        countDownLatch.trySetCount(1);
        final SimpleClient client = getClient();
        client.send(new CountDownRequest(name));
        client.receive();

        assertEquals(0, countDownLatch.getCount());
    }


}
