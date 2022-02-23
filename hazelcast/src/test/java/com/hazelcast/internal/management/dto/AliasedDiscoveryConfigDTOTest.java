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

import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.config.AzureConfig;
import com.hazelcast.config.ConfigCompatibilityChecker.AliasedDiscoveryConfigsChecker;
import com.hazelcast.config.EurekaConfig;
import com.hazelcast.config.GcpConfig;
import com.hazelcast.config.KubernetesConfig;
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
public class AliasedDiscoveryConfigDTOTest {

    @Test
    public void testSerialization() {
        GcpConfig expected = new GcpConfig().setEnabled(true)
                                            .setUsePublicIp(true)
                                            .setProperty("key1", "value1")
                                            .setProperty("key2", "value2");

        AliasedDiscoveryConfig actual = cloneThroughJson(expected);
        assertTrue("Expected: " + expected + ", got:" + actual,
                AliasedDiscoveryConfigsChecker.check(expected, actual));
    }

    @Test
    public void testDefault() {
        testDefault(new GcpConfig());
        testDefault(new AzureConfig());
        testDefault(new AwsConfig());
        testDefault(new EurekaConfig());
        testDefault(new KubernetesConfig());
    }

    private static void testDefault(AliasedDiscoveryConfig expected) {
        AliasedDiscoveryConfig actual = cloneThroughJson(expected);
        assertTrue("Expected: " + expected + ", got:" + actual,
                AliasedDiscoveryConfigsChecker.check(expected, actual));
    }

    private static AliasedDiscoveryConfig cloneThroughJson(AliasedDiscoveryConfig expected) {
        AliasedDiscoveryConfigDTO dto = new AliasedDiscoveryConfigDTO(expected);

        JsonObject json = dto.toJson();
        AliasedDiscoveryConfigDTO deserialized = new AliasedDiscoveryConfigDTO(expected.getTag());
        deserialized.fromJson(json);

        return deserialized.getConfig();
    }

}
