package com.hazelcast.topic.impl.reliable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ReliableTopicDestroyTest extends HazelcastTestSupport{

    private HazelcastInstance hz;
    private ReliableTopicProxy topic;
    private RingbufferService ringbufferService;

    @Before
    public void setup(){
        hz = createHazelcastInstance();
        topic = (ReliableTopicProxy)hz.getReliableTopic("foo");
        ringbufferService = getNodeEngineImpl(hz).getService(RingbufferService.SERVICE_NAME);
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
