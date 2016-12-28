package com.hazelcast.client.listeners;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TopicOnReconnectTest extends AbstractListenersOnReconnectTest {

    private ITopic topic;

    @Override
    protected String addListener() {
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