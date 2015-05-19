package com.hazelcast.topic.impl.reliable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ReliableMessageListener;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class DurableSubscriptionTest extends HazelcastTestSupport {

    @Test
    public void testDurableSubscription() {
        HazelcastInstance local = createHazelcastInstance();

        ITopic<String> topic = local.getReliableTopic("topic");
        final DurableMessageListener<String> listener = new DurableMessageListener<String>();

        String id = topic.addMessageListener(listener);
        topic.publish("item1");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(listener.objects.contains("item1"));
            }
        });

        topic.removeMessageListener(id);

        // todo: this part is still racy because we don't know when the listener is terminated.
        topic.publish("item2");
        topic.publish("item3");

        topic.addMessageListener(listener);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(asList("item1", "item2", "item3"), listener.objects);
                assertEquals(asList(0l, 1l, 2l), listener.sequences);
            }
        });
    }

    @Test
    public void beginFromStart() {

    }

    public class DurableMessageListener<String> implements ReliableMessageListener<String> {
        public final List<String> objects = new CopyOnWriteArrayList<String>();
        public final List<Long> sequences = new CopyOnWriteArrayList<Long>();
        public volatile long sequence = -1;

        @Override
        public long retrieveInitialSequence() {
            if (sequence == -1) {
                return -1;
            } else {
                // we need to add one because we want to read the next item.
                return sequence + 1;
            }
        }

        @Override
        public void storeSequence(long sequence) {
            sequences.add(sequence);
            this.sequence = sequence;
        }

        @Override
        public boolean isTerminal(Throwable failure) {
            return true;
        }

        @Override
        public boolean isLossTolerant() {
            return false;
        }

        @Override
        public void onMessage(Message<String> message) {
            objects.add(message.getMessageObject());
        }
    }
}
