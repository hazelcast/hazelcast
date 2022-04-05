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

package com.hazelcast.client.topic;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ReliableMessageListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

/**
 * Test for #7317 (https://github.com/hazelcast/hazelcast/issues/7317), contributed
 * by @nilskp (https://github.com/nilskp). Test retrieval of already published messages
 * after a loss-tolerant {@link ReliableMessageListener} registers with an initial
 * sequence outside head-tail sequence range, which results in a {@link com.hazelcast.ringbuffer.StaleSequenceException}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Issue7317Test extends HazelcastTestSupport {

    static final String smallRB = "foo";
    static final int smallRBCapacity = 3;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;

    @Before
    public void setup() {
        Config serverConfig = new Config();
        RingbufferConfig rbConf = new RingbufferConfig(smallRB);
        rbConf.setCapacity(smallRBCapacity);
        serverConfig.addRingBufferConfig(rbConf);
        hazelcastFactory.newHazelcastInstance(serverConfig);
        ClientConfig config = new ClientConfig();
        config.getReliableTopicConfig(smallRB).setReadBatchSize(smallRBCapacity);
        client = hazelcastFactory.newHazelcastClient(config);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    static class Issue7317MessageListener
            implements ReliableMessageListener<String> {

        private final List<String> messages;
        private final CountDownLatch cdl;
        private long seq;

        Issue7317MessageListener(List<String> messages, CountDownLatch cdl) {
            this.messages = messages;
            this.cdl = cdl;
        }

        public void onMessage(Message<String> msg) {
            assertEquals(messages.size() - cdl.getCount(), (int) seq);
            assertEquals(messages.get((int) seq), msg.getMessageObject());
            cdl.countDown();
        }

        public long retrieveInitialSequence() {
            return 0;
        }

        public void storeSequence(long sequence) {
            seq = sequence;
        }

        public boolean isLossTolerant() {
            return true;
        }

        public boolean isTerminal(Throwable failure) {
            return true;
        }
    }

    @Test
    public void registerListenerOnStaleSequenceClientServer() {
        final List<String> messages = Arrays.asList("a", "b", "c", "d", "e");
        final CountDownLatch cdl = new CountDownLatch(smallRBCapacity);
        ITopic<String> rTopic = client.getReliableTopic(smallRB);
        for (String message : messages) {
            rTopic.publish(message);
        }
        ReliableMessageListener<String> listener = new Issue7317MessageListener(messages, cdl);
        UUID reg = rTopic.addMessageListener(listener);
        assertOpenEventually(cdl);
        rTopic.removeMessageListener(reg);

    }
}
