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

package com.hazelcast.client.topic;/*
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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;

/**
 * Test for #9766 (https://github.com/hazelcast/hazelcast/issues/9766)
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})

public class Issue9766Test {
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    static final String topicName = "foo";

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void serverRestartWhenReliableTopicListenerRegistered() {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();

        HazelcastInstance hazelcastClient = hazelcastFactory.newHazelcastClient();
        HazelcastInstance hazelcastClient2 = hazelcastFactory.newHazelcastClient();
        ITopic<Integer> topic = hazelcastClient.getReliableTopic(topicName);
        final ITopic<Integer> topic2 = hazelcastClient2.getReliableTopic(topicName);

        final CountDownLatch listenerLatch = new CountDownLatch(1);

        // Add listener using the first client
        topic.addMessageListener(new MessageListener<Integer>() {
            @Override
            public void onMessage(Message<Integer> message) {
                listenerLatch.countDown();
            }
        });

        // restart the server
        server.getLifecycleService().terminate();

        hazelcastFactory.newHazelcastInstance();

        // publish some data
        topic2.publish(5);

        assertOpenEventually(listenerLatch);
    }
}
