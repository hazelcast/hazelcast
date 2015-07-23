package com.hazelcast.client.listeners;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TopicOnReconnectTest extends AbstractListenersOnReconnectTest {

    private ITopic topic;

    @Override
    protected String addListener(final AtomicInteger eventCount) {
        topic = client.getTopic(randomString());
        MessageListener listener = new MessageListener() {
            @Override
            public void onMessage(Message message) {
                eventCount.incrementAndGet();
            }
        };
        return topic.addMessageListener(listener);
    }

    @Override
    public void produceEvent() {
        topic.publish(randomString());
    }

    @Override
    public boolean removeListener(String registrationId) {
        return topic.removeMessageListener(registrationId);
    }
}