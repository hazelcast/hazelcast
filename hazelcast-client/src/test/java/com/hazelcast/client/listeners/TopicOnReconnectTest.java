/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.listeners;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.impl.TopicService;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TopicOnReconnectTest extends AbstractListenersOnReconnectTest {

    private ITopic<String> topic;

    @Override
    String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    @Override
    protected String addListener() {
        topic = client.getTopic(randomString());

        MessageListener<String> listener = new MessageListener<String>() {
            @Override
            public void onMessage(Message<String> message) {
                onEvent(message.getMessageObject());
            }
        };
        return topic.addMessageListener(listener);
    }

    @Override
    public void produceEvent(String event) {
        topic.publish(event);
    }

    @Override
    public boolean removeListener(String registrationId) {
        return topic.removeMessageListener(registrationId);
    }
}
