package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LossToleranceTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private ReliableTopicProxy<String> topic;
    private Ringbuffer<ReliableTopicMessage> ringbuffer;

    @Before
    public void setup() {
        setLogLevel(Level.DEBUG);
        Config config = new Config();
        config.addRingBufferConfig(new RingbufferConfig("foo")
                .setCapacity(100)
                .setTimeToLiveSeconds(0));
        hz = createHazelcastInstance(config);

        topic = (ReliableTopicProxy) hz.getReliableTopic("foo");
        ringbuffer = topic.ringbuffer;
    }

    @Test
    public void whenNotLossTolerant_thenTerminate() {
        ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        listener.initialSequence = 0;
        listener.isLossTolerant = false;

        topic.publish("foo");


        int k = 0;
        // we add so many items that the items the listener wants to listen to, doesn't exist anymore
        for (; ; ) {
            topic.publish("item");
            if (ringbuffer.headSequence() > listener.initialSequence) {
                break;
            }
            k++;
        }
        topic.addMessageListener(listener);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(topic.runnersMap.isEmpty());
            }
        });
    }

    @Test
    public void whenLossTolerant_thenContinue() {
        final ReliableMessageListenerMock listener = new ReliableMessageListenerMock();
        listener.initialSequence = 0;
        listener.isLossTolerant = true;

        // we add so many items that the items the listener wants to listen to, doesn't exist anymore
        for (; ; ) {
            topic.publish("item");
            if (ringbuffer.headSequence() > listener.initialSequence) {
                break;
            }
        }

        topic.addMessageListener(listener);
        topic.publish("newitem");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(listener.objects.contains("newitem"));
                assertFalse(topic.runnersMap.isEmpty());
            }
        });
    }
}
