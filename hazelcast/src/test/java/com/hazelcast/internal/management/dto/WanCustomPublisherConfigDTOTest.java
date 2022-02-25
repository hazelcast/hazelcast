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

package com.hazelcast.internal.management.dto;

import com.hazelcast.config.ConfigCompatibilityChecker.WanCustomPublisherConfigChecker;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanCustomPublisherConfigDTOTest {

    private static final WanCustomPublisherConfigChecker WAN_PUBLISHER_CONFIG_CHECKER = new WanCustomPublisherConfigChecker();

    @Test
    public void testSerialization() {
        Map<String, Comparable> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        WanCustomPublisherConfig expected = new WanCustomPublisherConfig()
                .setPublisherId("myPublisherId")
                .setClassName("className")
                .setProperties(properties);

        WanCustomPublisherConfig actual = cloneThroughJson(expected);
        assertTrue("Expected: " + expected + ", got:" + actual,
                WAN_PUBLISHER_CONFIG_CHECKER.check(expected, actual));
    }

    @Test
    public void testDefault() {
        WanCustomPublisherConfig expected = new WanCustomPublisherConfig();

        WanCustomPublisherConfig actual = cloneThroughJson(expected);
        assertTrue("Expected: " + expected + ", got:" + actual,
                WAN_PUBLISHER_CONFIG_CHECKER.check(expected, actual));
    }

    private WanCustomPublisherConfig cloneThroughJson(WanCustomPublisherConfig expected) {
        CustomWanPublisherConfigDTO dto = new CustomWanPublisherConfigDTO(expected);

        JsonObject json = dto.toJson();
        CustomWanPublisherConfigDTO deserialized = new CustomWanPublisherConfigDTO(null);
        deserialized.fromJson(json);

        return deserialized.getConfig();
    }

}
