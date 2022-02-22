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

import com.hazelcast.config.ConfigCompatibilityChecker.DiscoveryConfigChecker;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DiscoveryConfigDTOTest {

    private static final DiscoveryConfigChecker DISCOVERY_CONFIG_CHECKER = new DiscoveryConfigChecker();

    @Test
    public void testSerialization() {
        DiscoveryConfig expected = new DiscoveryConfig();
        expected.setNodeFilterClass("myClassName");
        expected.addDiscoveryStrategyConfig(new DiscoveryStrategyConfig("className1"));
        expected.addDiscoveryStrategyConfig(new DiscoveryStrategyConfig("className2"));

        DiscoveryConfig actual = cloneThroughJson(expected);
        assertTrue("Expected: " + expected + ", got:" + actual,
                DISCOVERY_CONFIG_CHECKER.check(expected, actual));
    }

    @Test
    public void testDefault() {
        DiscoveryConfig expected = new DiscoveryConfig();

        DiscoveryConfig actual = cloneThroughJson(expected);
        assertTrue("Expected: " + expected + ", got:" + actual,
                DISCOVERY_CONFIG_CHECKER.check(expected, actual));
    }

    private DiscoveryConfig cloneThroughJson(DiscoveryConfig expected) {
        DiscoveryConfigDTO dto = new DiscoveryConfigDTO(expected);

        JsonObject json = dto.toJson();
        DiscoveryConfigDTO deserialized = new DiscoveryConfigDTO(null);
        deserialized.fromJson(json);

        return deserialized.getConfig();
    }

}
