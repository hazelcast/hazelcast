package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadException;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TopicOverloadTest extends HazelcastTestSupport {

    private ReliableTopicProxy<String> topic;
    private Ringbuffer<ReliableTopicMessage> ringbuffer;
    private SerializationService serializationService;
    private RingbufferService ringbufferService;
    private RingbufferContainer ringbufferContainer;

    @Before
    public void setup() {
        Config config = new Config();

        config.addRingBufferConfig(new RingbufferConfig("when*")
                .setCapacity(100).setTimeToLiveSeconds(5));
        config.addReliableTopicConfig(new ReliableTopicConfig("whenError_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.ERROR));
        config.addReliableTopicConfig(new ReliableTopicConfig("whenDiscardOldest_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST));
        config.addReliableTopicConfig(new ReliableTopicConfig("whenDiscardNewest_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_NEWEST));
        config.addReliableTopicConfig(new ReliableTopicConfig("whenBlock_*")
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK));

        HazelcastInstance hz = createHazelcastInstance(config);

        serializationService = getSerializationService(hz);

        String topicName = getTestMethodName();
        topic = (ReliableTopicProxy)hz.getReliableTopic(topicName);

        ringbuffer = topic.ringbuffer;

        ringbufferService = getNodeEngineImpl(hz).getService(RingbufferService.SERVICE_NAME);
        ringbufferContainer = ringbufferService.getContainer(ringbuffer.getName());
    }

    @Test
    public void whenError_andSpace() throws Exception {
        test_whenSpace();
    }

    @Test
    public void whenDiscardNewest_andSpace() throws Exception {
        test_whenSpace();
    }

    @Test
    public void whenDiscardOldest_andSpace() throws Exception {
        test_whenSpace();
    }

    public void test_whenSpace() throws Exception {
        topic.publish("foo");

        ReliableTopicMessage msg = ringbuffer.readOne(0);
        assertEquals("foo", serializationService.toObject(msg.getPayload()));
    }

    @Test
    public void whenError_andNoSpace() {
        for (int k = 0; k < ringbuffer.capacity(); k++) {
            topic.publish("old");
        }

        long tail = ringbuffer.tailSequence();
        long head = ringbuffer.headSequence();

        try {
            topic.publish("new");
            fail();
        } catch (TopicOverloadException expected) {
        }

        assertEquals(tail, ringbuffer.tailSequence());
        assertEquals(head, ringbuffer.headSequence());
    }

    @Test
    public void whenDiscardOldest_whenNoSpace() {
        for (int k = 0; k < ringbuffer.capacity(); k++) {
            topic.publish("old");
        }

        long tail = ringbuffer.tailSequence();
        long head = ringbuffer.headSequence();

        topic.publish("new");

        // check that an item has been added.
        assertEquals(tail + 1, ringbuffer.tailSequence());
        assertEquals(head + 1, ringbuffer.headSequence());
    }

    @Test
    public void whenDiscardNewest_whenNoSpace() {
        for (int k = 0; k < ringbuffer.capacity(); k++) {
            topic.publish("old");
        }

        long tail = ringbuffer.tailSequence();
        long head = ringbuffer.headSequence();

        topic.publish("new");

        // check that nothing has changed
        assertEquals(tail, ringbuffer.tailSequence());
        assertEquals(head, ringbuffer.headSequence());
    }

    @Test
    public void whenBlock_whenNoSpace() {


        for (int k = 0; k < ringbuffer.capacity(); k++) {
            topic.publish("old");
        }

        final long tail = ringbuffer.tailSequence();
        final long head = ringbuffer.headSequence();

        // add the item.
        final Future f = spawn(new Runnable() {
            @Override
            public void run() {
                topic.publish("new");
            }
        });

        // make sure it doesn't complete within 3 seconds. We have a 2 second error margin to prevent spurious test failures
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(f.isDone());
                assertEquals(tail, ringbuffer.tailSequence());
                assertEquals(head, ringbuffer.headSequence());
            }
        }, 3);

        assertCompletesEventually(f);

        assertEquals(tail + 1, ringbuffer.tailSequence());
        // since the ringbuffer got cleaned, the head is at the tail
        assertEquals(tail + 1, ringbuffer.headSequence());
    }
}
