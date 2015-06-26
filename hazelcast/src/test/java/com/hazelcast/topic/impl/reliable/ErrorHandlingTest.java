package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Message;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ErrorHandlingTest extends HazelcastTestSupport {

    private HazelcastInstance hz;
    private ReliableTopicProxy<String> topic;
    private Ringbuffer<ReliableTopicMessage> ringbuffer;

    @Before
    public void setup() {
        Config config = new Config();
        config.addRingBufferConfig(new RingbufferConfig("foo")
                .setCapacity(100)
                .setTimeToLiveSeconds(0));
        hz = createHazelcastInstance(config);
        topic = (ReliableTopicProxy) hz.getReliableTopic("foo");
        ringbuffer = topic.ringbuffer;
    }

    @Test
    public void isTerminal_throwsException_thenTerminate() {
        final ErrorListenerMock listener = new ErrorListenerMock();
        listener.throwErrorOnIsTerminal = true;
        listener.isTerminal = true;
        topic.addMessageListener(listener);

        topic.publish("item1");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.objects.size());
                assertTrue(topic.runnersMap.isEmpty());
            }
        });

        topic.publish("item2");

        // we need to make sure we don't receive item 2 since the listener is terminated.
        assertTrueFiveSeconds(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.objects.size());
            }
        });
    }

    @Test
    public void whenOnMessageThrowsException_andTerminal_thenTerminated() {
        final ErrorListenerMock listener = new ErrorListenerMock();
        listener.isTerminal = true;
        topic.addMessageListener(listener);

        topic.publish("item1");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.objects.size());
                assertTrue(topic.runnersMap.isEmpty());
            }
        });

        topic.publish("item2");

        // we need to make sure we don't receive item 2 since the listener is terminated.
        assertTrueFiveSeconds(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.objects.size());
            }
        });
    }

    @Test
    public void whenOnMessageThrowsException_andNotTerminal_thenListenerDoesNotTerminate() {
        final ErrorListenerMock listener = new ErrorListenerMock();
        listener.isTerminal = false;
        topic.addMessageListener(listener);

        topic.publish("item1");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, listener.objects.size());
                assertFalse(topic.runnersMap.isEmpty());
            }
        });

        topic.publish("item2");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, listener.objects.size());
                assertFalse(topic.runnersMap.isEmpty());
            }
        });
    }


    public class ErrorListenerMock extends ReliableMessageListenerMock {

        private volatile boolean throwErrorOnIsTerminal = false;

        @Override
        public void onMessage(Message<String> message) {
            super.onMessage(message);
            throw new ExpectedRuntimeException();
        }

        @Override
        public boolean isTerminal(Throwable failure) {
            if (throwErrorOnIsTerminal) {
                throw new ExpectedRuntimeException();
            }
            return super.isTerminal(failure);
        }
    }
}
