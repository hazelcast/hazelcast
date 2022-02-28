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

package com.hazelcast.internal.jmx;

import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.LocalTopicStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReliableTopicMBeanTest extends HazelcastTestSupport {

    private static final String TYPE_NAME = "ReliableTopic";

    private TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(1);
    private MBeanDataHolder holder = new MBeanDataHolder(hazelcastInstanceFactory);

    private ITopic<String> reliableTopic;
    private String objectName;

    @Before
    public void setUp() {
        reliableTopic = holder.getHz().getReliableTopic("reliableTopic");
        objectName = reliableTopic.getName();
        holder.assertMBeanExistEventually(TYPE_NAME, reliableTopic.getName());
    }

    @After
    public void tearDown() {
        hazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void testName() throws Exception {
        String name = getStringAttribute("name");
        assertEquals("reliableTopic", name);
    }

    @Test
    public void testConfig() throws Exception {
        String config = getStringAttribute("config");
        assertTrue("configuration string should start with 'ReliableTopicConfig{'", config.startsWith("ReliableTopicConfig{"));
    }

    @Test
    public void testAttributesAndOperations() throws Exception {
        reliableTopic.publish("test");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                LocalTopicStats topicStats = reliableTopic.getLocalTopicStats();
                long localCreationTime = getLongAttribute("localCreationTime");
                long localPublishOperationCount = getLongAttribute("localPublishOperationCount");
                long localReceiveOperationCount = getLongAttribute("localReceiveOperationCount");

                assertEquals(topicStats.getCreationTime(), localCreationTime);
                assertEquals(topicStats.getPublishOperationCount(), localPublishOperationCount);
                assertEquals(topicStats.getReceiveOperationCount(), localReceiveOperationCount);
            }
        });
    }

    private String getStringAttribute(String name) throws Exception {
        return (String) holder.getMBeanAttribute(TYPE_NAME, objectName, name);
    }

    private Long getLongAttribute(String name) throws Exception {
        return (Long) holder.getMBeanAttribute(TYPE_NAME, objectName, name);
    }
}
