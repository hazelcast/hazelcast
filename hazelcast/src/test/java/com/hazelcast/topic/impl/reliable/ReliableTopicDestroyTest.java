package com.hazelcast.topic.impl.reliable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReliableTopicDestroyTest extends HazelcastTestSupport {

    private ReliableTopicProxy<String> topic;
    private RingbufferService ringbufferService;

    @Before
    public void setup() {
        setLoggingLog4j();
        setLogLevel(Level.TRACE);

        HazelcastInstance hz = createHazelcastInstance();
        topic = (ReliableTopicProxy<String>) hz.<String>getReliableTopic("foo");
        ringbufferService = getNodeEngineImpl(hz).getService(RingbufferService.SERVICE_NAME);
    }

    @After
    public void teardown() {
        resetLogLevel();
    }

    @Test
    public void whenDestroyedThenListenersTerminate() {
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        topic.addMessageListener(listener);

        sleepSeconds(4);

        topic.destroy();

        System.out.println("Destroyed; now a bit of waiting");
        sleepSeconds(4);

        listener.clean();

        topic.publish("foo");

        // it should not receive any events.
        assertTrueDelayed5sec(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, listener.objects.size());
            }
        });
    }

    @Test
    public void whenDestroyedThenRingbufferRemoved() {
        topic.publish("foo");
        topic.destroy();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                ConcurrentMap<String, RingbufferContainer> containers = ringbufferService.getContainers();
                assertFalse(containers.containsKey(topic.ringbuffer.getName()));
            }
        });
    }
}
