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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.topic.ITopic;
import org.junit.Test;

public abstract class AbstractReliableTopicNullTest extends HazelcastTestSupport {

    @Test(expected = NullPointerException.class)
    public void testAddNullMessageListener() {
        ITopic<Object> topic = getDriver().getReliableTopic(randomName());
        topic.addMessageListener(null);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveMessageListenerWithNullId() {
        ITopic<Object> topic = getDriver().getReliableTopic(randomName());
        topic.removeMessageListener(null);
    }

    @Test(expected = NullPointerException.class)
    public void testPublishNullMessage() {
        ITopic<Object> topic = getDriver().getReliableTopic(randomName());
        topic.publish(null);
    }

    protected abstract HazelcastInstance getDriver();
}
