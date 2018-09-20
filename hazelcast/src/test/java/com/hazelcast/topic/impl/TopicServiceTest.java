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

package com.hazelcast.topic.impl;

import static org.junit.Assert.assertFalse;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TopicServiceTest extends HazelcastTestSupport {

    @Test
    public void testDestroyTopicRemovesStatistics() {
        String randomTopicName = randomString();

        HazelcastInstance instance = createHazelcastInstance();
        final ITopic<String> topic = instance.getTopic(randomTopicName);
        topic.publish("foobar");

        // we need to give the message the chance to be processed, else the topic statistics are recreated
        // so in theory the destroy for the topic is broken
        sleepSeconds(1);

        topic.destroy();

        final TopicService topicService = getNode(instance).nodeEngine.getService(TopicService.SERVICE_NAME);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                boolean containsStats = topicService.getStatsMap().containsKey(topic.getName());
                assertFalse(containsStats);
            }
        });
    }
}
