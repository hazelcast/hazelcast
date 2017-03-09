/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.NightlyTest;
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

    @Test(timeout = 600000)
    public void test() throws InterruptedException {
        final StressMessageListener listener1 = new StressMessageListener(1);
        topic.addMessageListener(listener1);
        final StressMessageListener listener2 = new StressMessageListener(2);
        topic.addMessageListener(listener2);

        final ProduceThread produceThread = new ProduceThread();
        produceThread.start();

        System.out.println("Starting test");
        sleepAndStop(stop, MINUTES.toSeconds(5));
        System.out.println("Completed");

        produceThread.assertSucceedsEventually();

        System.out.println("Number of items produced: " + produceThread.send);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(produceThread.send, listener1.received);
                assertEquals(produceThread.send, listener2.received);
                assertEquals(0, listener1.failures);
                assertEquals(0, listener2.failures);
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
        public void doRun() throws Throwable {
            while (!stop.get()) {
                topic.publish(send);
                send++;
            }
        }
    }

    public class StressMessageListener implements MessageListener<Long> {
        private final int id;
        private long received = 0;
        private long failures = 0;

        public StressMessageListener(int id) {
            this.id = id;
        }

        @Override
        public void onMessage(Message<Long> message) {
            if (!message.getMessageObject().equals(received)) {
                failures++;
            }

            if (received % 100000 == 0) {
                System.out.println(toString() + " is at: " + received);
            }

            received++;
        }

        @Override
        public String toString() {
            return "StressMessageListener{" +
                    "id=" + id +
                    '}';
        }
    }
}
