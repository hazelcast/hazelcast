/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.topic.ReliableMessageListener;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class ReliableTopicStressTest extends HazelcastTestSupport {

    private final ILogger logger = Logger.getLogger(ReliableTopicStressTest.class);
    private final AtomicBoolean stop = new AtomicBoolean();
    private ITopic<Long> topic;

    @Before
    public void setup() {
        Config config = new Config();

        RingbufferConfig ringbufferConfig = new RingbufferConfig("foobar");
        ringbufferConfig.setCapacity(1000 * 1000);
        ringbufferConfig.setTimeToLiveSeconds(5);
        config.addRingBufferConfig(ringbufferConfig);

        TopicConfig topicConfig = new TopicConfig("foobar");
        config.addTopicConfig(topicConfig);

        HazelcastInstance hz = createHazelcastInstance(config);
        topic = hz.getReliableTopic(topicConfig.getName());
    }

    @Test(timeout = 1000 * 60 * 10)
    public void test() {
        final StressMessageListener listener1 = new StressMessageListener(1);
        topic.addMessageListener(listener1);
        final StressMessageListener listener2 = new StressMessageListener(2);
        topic.addMessageListener(listener2);

        final ProduceThread produceThread = new ProduceThread();
        produceThread.start();

        logger.info("Starting test");
        sleepAndStop(stop, MINUTES.toSeconds(5));
        logger.info("Waiting for completion");

        produceThread.assertSucceedsEventually();

        logger.info("Number of items produced: " + produceThread.send);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(produceThread.send, listener1.received + listener1.lost);
                assertEquals(produceThread.send, listener2.received + listener2.lost);
                assertEquals(0, listener1.failed);
                assertEquals(0, listener2.failed);
            }
        });
    }

    public class ProduceThread extends TestThread {
        private volatile long send = 0;

        @Override
        public void onError(Throwable t) {
            stop.set(true);
        }

        @Override
        public void doRun() {
            while (!stop.get()) {
                topic.publish(send);
                send++;
            }
        }
    }

    public class StressMessageListener implements ReliableMessageListener<Long> {
        private final int id;
        private long nextExpectedMessageId = 0; // what's the next expected message ID
        private long lost = 0; // how many were lost because of slow listener (listener was slow)
        private long received = 0; // how many were successfully received (listener was fast enough)
        private long failed = 0; // how many times did the listener fail (listener was fast but got wrong message ID)
        private boolean listenerWasSlow; // was the listener slow?

        public StressMessageListener(int id) {
            this.id = id;
        }

        @Override
        public void onMessage(Message<Long> message) {
            final long receivedMessageId = message.getMessageObject();
            if (receivedMessageId != nextExpectedMessageId) {
                if (listenerWasSlow) {
                    logger.info(toString() + " was slow, jumping from " + received + " to " + receivedMessageId);
                    lost += receivedMessageId - nextExpectedMessageId;
                    nextExpectedMessageId = receivedMessageId;
                    listenerWasSlow = false;
                } else {
                    failed++;
                }
            }

            if (received % 100000 == 0) {
                logger.info(toString() + " is at: " + received);
            }

            received++;
            nextExpectedMessageId++;
        }

        @Override
        public String toString() {
            return "StressMessageListener{"
                    + "id=" + id
                    + '}';
        }

        @Override
        public long retrieveInitialSequence() {
            // -1 indicates start from next message.
            return -1;
        }

        @Override
        public void storeSequence(long sequence) {
            //np-op
        }

        @Override
        public boolean isLossTolerant() {
            listenerWasSlow = true;
            return true;
        }

        @Override
        public boolean isTerminal(Throwable failure) {
            return false;
        }
    }
}
