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

import com.hazelcast.config.ConfigCompatibilityChecker.WanReplicationConfigChecker;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanConsumerConfig;
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
public class WanReplicationConfigDTOTest {

    private static final WanReplicationConfigChecker WAN_REPLICATION_CONFIG_CHECKER = new WanReplicationConfigChecker();

    @Test
    public void testSerialization() {
        WanReplicationConfig expected = new WanReplicationConfig()
                .setName("myName")
                .setConsumerConfig(new WanConsumerConfig())
                .addBatchReplicationPublisherConfig(new WanBatchPublisherConfig()
                        .setClusterName("group1"))
                .addCustomPublisherConfig(new WanCustomPublisherConfig()
                        .setPublisherId("group2")
                        .setClassName("className"));

        WanReplicationConfig actual = cloneThroughJson(expected);

        assertTrue("Expected: " + expected + ", got:" + actual,
                WAN_REPLICATION_CONFIG_CHECKER.check(expected, actual));
    }

    private WanReplicationConfig cloneThroughJson(WanReplicationConfig expectedConfig) {
        WanReplicationConfigDTO dto = new WanReplicationConfigDTO(expectedConfig);

        JsonObject json = dto.toJson();
        WanReplicationConfigDTO deserialized = new WanReplicationConfigDTO(null);
        deserialized.fromJson(json);

        return deserialized.getConfig();
    }
}
