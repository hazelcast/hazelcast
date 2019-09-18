/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.config.ConfigCompatibilityChecker.WanCustomPublisherConfigChecker;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CustomWanPublisherConfigTest {

    private CustomWanPublisherConfig config = new CustomWanPublisherConfig();
    private static final WanCustomPublisherConfigChecker WAN_PUBLISHER_CONFIG_CHECKER
            = new WanCustomPublisherConfigChecker();

    @Test
    public void testSerialization() {
        Map<String, Comparable> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        CustomWanPublisherConfig config = new CustomWanPublisherConfig()
                .setPublisherId("myPublisherId")
                .setClassName("className")
                .setProperties(properties)
                .setImplementation("implementation");

        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(config);
        CustomWanPublisherConfig deserialized = serializationService.toObject(serialized);

        assertWanPublisherConfig(config, deserialized);
    }

    private static void assertWanPublisherConfig(CustomWanPublisherConfig expected,
                                                 CustomWanPublisherConfig actual) {
        WAN_PUBLISHER_CONFIG_CHECKER.check(expected, actual);
        assertEquals(expected.toString(), actual.toString());
    }
}
