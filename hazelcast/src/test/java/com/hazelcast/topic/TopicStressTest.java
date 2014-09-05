package com.hazelcast.topic;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

/**
 * This test creates a cluster of HazelcastInstances and a bunch of Topics.
 *
 * On each instance, there is a listener for each topic.
 *
 * There is a bunch of threads, that selects a random instance with a random topic to publish a message (int) on.
 *
 * To verify that everything is fine, we expect that the total messages send by each topic is the same as the total
 * sum of messages receives by the topic listeners.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class TopicStressTest extends HazelcastTestSupport {

    public static final int PUBLISH_THREAD_COUNT = 10;
    public static final int NODE_COUNT = 10;
    public static final int TOPIC_COUNT = 10;
    public static final int RUNNING_TIME_SECONDS = 600;
    //if we set this value very low, it could be that events are dropped due to overload of the event queue.
    public static final int MAX_PUBLISH_DELAY_MILLIS = 25;

    private HazelcastInstance[] instances;
    private CountDownLatch startLatch;
    private PublishThread[] publishThreads;
    private Map<String, List<MessageListenerImpl>> listenerMap;

    @Before
    public void setUp() {
        instances = createHazelcastInstanceFactory(NODE_COUNT).newInstances();

        startLatch = new CountDownLatch(1);
        publishThreads = new PublishThread[PUBLISH_THREAD_COUNT];
        for (int k = 0; k < publishThreads.length; k++) {
            PublishThread publishThread = new PublishThread(startLatch);
            publishThread.start();
            publishThreads[k] = publishThread;
        }

        listenerMap = new HashMap<String, List<MessageListenerImpl>>();
        for (int topicIndex = 0; topicIndex < TOPIC_COUNT; topicIndex++) {
            String topicName = getTopicName(topicIndex);
            List<MessageListenerImpl> listeners = registerTopicListeners(topicName, instances);
            listenerMap.put(topicName, listeners);
        }
    }

    @Test
    public void test() throws Exception {
        startLatch.countDown();
        System.out.printf("Test is going to run for %s seconds\n", RUNNING_TIME_SECONDS);

        for (PublishThread thread : publishThreads) {
            thread.join();
        }

        System.out.println("All publish threads have completed");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int topicIndex = 0; topicIndex < TOPIC_COUNT; topicIndex++) {
                    String topicName = getTopicName(topicIndex);
                    long expected = getExpectedCount(topicName);
                    long actual = getActualCount(topicName);
                    assertEquals("Count for topic " + topicName + " is not the same", expected,actual);
                }
            }
        });
    }

    private long getExpectedCount(String topic) {
        long result = 0;
        for (PublishThread publishThread : publishThreads) {
            Long count = publishThread.messageCount.get(topic);
            if (count != null) {
                result += count;
            }
        }

        //since each message is send to multiple nodes.. we need to multiply it
        return result * NODE_COUNT;
    }

    private long getActualCount(String topic) {
        long result = 0;

        List<MessageListenerImpl> listeners = listenerMap.get(topic);
        if (listeners == null) {
            return 0;
        }

        for (MessageListenerImpl listener : listeners) {
            result += listener.counter.get();
        }
        return result;
    }

    private String getTopicName(int topicIndex) {
        return "topic" + topicIndex;
    }

    private class PublishThread extends Thread {
        private final Random random = new Random();
        private final Map<String, Long> messageCount = new HashMap<String, Long>();
        private final CountDownLatch startLatch;

        private PublishThread(CountDownLatch startLatch) {
            this.startLatch = startLatch;
        }

        @Override
        public void run() {
            try {
                startLatch.await();

                long endTimeMillis = getEndTimeMillis();
                while (System.currentTimeMillis() < endTimeMillis) {
                    HazelcastInstance instance = randomInstance();
                    ITopic<Integer> topic = randomTopic(instance);
                    int inc = random.nextInt(100);
                    topic.publish(inc);
                    updateCount(topic, inc);
                    randomSleep();
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        private long getEndTimeMillis() {
            return System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(RUNNING_TIME_SECONDS);
        }

        private void updateCount(ITopic<Integer> topic, int inc) {
            String topicName = topic.getName();
            Long count = messageCount.get(topicName);
            if (count == null) {
                count = 0l;
            }
            count += inc;
            messageCount.put(topicName, count);
        }

        private void randomSleep() {
            try {
                Thread.sleep(random.nextInt(MAX_PUBLISH_DELAY_MILLIS));
            } catch (InterruptedException e) {
            }
        }

        private ITopic<Integer> randomTopic(HazelcastInstance instance) {
            String randomTopicName = getTopicName(random.nextInt(TOPIC_COUNT));
            return instance.getTopic(randomTopicName);
        }

        private HazelcastInstance randomInstance() {
            int index = random.nextInt(instances.length);
            return instances[index];
        }
    }

    private List<MessageListenerImpl> registerTopicListeners(String topicName, HazelcastInstance[] instances) {
        List<MessageListenerImpl> listeners = new LinkedList<MessageListenerImpl>();
        for (HazelcastInstance hz : instances) {
            MessageListenerImpl listener = new MessageListenerImpl();
            ITopic<Integer> topic = hz.getTopic(topicName);
            topic.addMessageListener(listener);
            listeners.add(listener);
        }
        return listeners;
    }

    private class MessageListenerImpl implements MessageListener<Integer> {
        private final AtomicLong counter = new AtomicLong();

        @Override
        public void onMessage(Message<Integer> message) {
            int inc = message.getMessageObject();
            counter.addAndGet(inc);
        }
    }
}
