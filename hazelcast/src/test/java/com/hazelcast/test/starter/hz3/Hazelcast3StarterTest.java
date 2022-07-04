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

package com.hazelcast.test.starter.hz3;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.assertj.core.api.Assertions.assertThat;

@Category({QuickTest.class})
public class Hazelcast3StarterTest {

    private static final String HZ3_MEMBER_CONFIG =
            "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\"\n"
            + "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "           xsi:schemaLocation=\"http://www.hazelcast.com/schema/config\n"
            + "           http://www.hazelcast.com/schema/config/hazelcast-config-3.12.xsd\">\n"
            + "    <group>\n"
            + "        <name>dev</name>\n"
            + "    </group>\n"
            + "    <network>\n"
            + "        <port auto-increment=\"true\" port-count=\"100\">3210</port>\n"
            + "        <join>\n"
            + "            <multicast enabled=\"false\">\n"
            + "            </multicast>\n"
            + "        </join>\n"
            + "    </network>\n"
            + "</hazelcast>\n";

    @Test
    public void testMapPutGet() {
        HazelcastInstance instance = Hazelcast3Starter.newHazelcastInstance(HZ3_MEMBER_CONFIG);
        IMap<String, String> map = instance.getMap("my-map");
        map.put("key", "value");
        String value = map.get("key");
        assertThat(value).isEqualTo("value");

        instance.shutdown();
    }

    @Test
    public void testTopicMessageListenerPublish() {
        HazelcastInstance instance = Hazelcast3Starter.newHazelcastInstance(HZ3_MEMBER_CONFIG);
        ITopic<String> topic = instance.getTopic("my-topic");
        List<String> result = new ArrayList<>();
        topic.addMessageListener(message -> {
            result.add(message.getMessageObject());
        });
        topic.publish("value");
        assertTrueEventually(
                () -> assertThat(result).contains("value"),
                5
        );

        instance.shutdown();
    }
}
