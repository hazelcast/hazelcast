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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.impl.reliable.ReliableMessageListenerMock;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.topic.impl.reliable.ReliableTopicService.SERVICE_NAME;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReliableTopicStatsTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance instance;
    private ITopic<String> topic;

    @Before
    public void setup() {
        instance = hazelcastFactory.newHazelcastInstance(new Config());
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        String name = randomMapName("reliableTopic-");
        topic = client.getReliableTopic(name);
        topic.addMessageListener(new ReliableMessageListenerMock());
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void publish_countStats() {
        topic.publish("message1");
        topic.publishAsync("message2");

        assertTrueEventually(() -> {
            ReliableTopicService service = getNode(instance).getNodeEngine().getService(SERVICE_NAME);
            LocalTopicStatsImpl localTopicStats = service.getLocalTopicStats(topic.getName());
            assertEquals(2, localTopicStats.getPublishOperationCount());
            assertEquals(2, localTopicStats.getReceiveOperationCount());
        });
    }

    @Test
    public void publishAll_countStats() throws Exception {
        topic.publishAll(asList("message1", "message2", "message3"));
        topic.publishAllAsync(asList("message4", "message5", "message6"));

        assertTrueEventually(() -> {
            ReliableTopicService service = getNode(instance).getNodeEngine().getService(SERVICE_NAME);
            LocalTopicStatsImpl localTopicStats = service.getLocalTopicStats(topic.getName());
            assertEquals(6, localTopicStats.getPublishOperationCount());
            assertEquals(6, localTopicStats.getReceiveOperationCount());
        });
    }

}
