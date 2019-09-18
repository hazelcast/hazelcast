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

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.config.WanBatchReplicationPublisherConfigTest.assertWanPublisherConfig;
import static com.hazelcast.config.WanConsumerConfigTest.assertWanConsumerConfig;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanReplicationConfigTest {

    private WanReplicationConfig config = new WanReplicationConfig();

    @Test
    public void testSerialization() {
        Map<String, Comparable> properties = new HashMap<>();
        properties.put("key", "value");

        WanConsumerConfig wanConsumerConfig = new WanConsumerConfig();
        wanConsumerConfig.setProperties(properties);
        wanConsumerConfig.setClassName("className");
        wanConsumerConfig.setImplementation("implementation");

        WanBatchReplicationPublisherConfig wanPublisherConfig1 = new WanBatchReplicationPublisherConfig();
        WanBatchReplicationPublisherConfig wanPublisherConfig2 = new WanBatchReplicationPublisherConfig();

        List<WanBatchReplicationPublisherConfig> publisherConfigs = new LinkedList<>();
        publisherConfigs.add(wanPublisherConfig1);

        config.setName("name");
        config.setWanConsumerConfig(wanConsumerConfig);
        config.setBatchPublisherConfigs(publisherConfigs);
        config.addWanBatchReplicationPublisherConfig(wanPublisherConfig2);

        ArrayList<CustomWanPublisherConfig> customPublishers = new ArrayList<>(1);
        customPublishers.add(new CustomWanPublisherConfig());
        config.setCustomPublisherConfigs(customPublishers);
        config.addCustomPublisherConfig(new CustomWanPublisherConfig());

        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(config);
        WanReplicationConfig deserialized = serializationService.toObject(serialized);

        assertWanReplicationConfig(config, deserialized);
    }

    @Test
    public void testSerialization_withEmpyConfigs() {
        config.setName("name");

        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(config);
        WanReplicationConfig deserialized = serializationService.toObject(serialized);

        assertWanReplicationConfig(config, deserialized);
    }

    private static void assertWanReplicationConfig(WanReplicationConfig expected, WanReplicationConfig actual) {
        assertEquals(expected.getName(), actual.getName());
        assertWanConsumerConfig(expected.getWanConsumerConfig(), actual.getWanConsumerConfig());
        Iterator<WanBatchReplicationPublisherConfig> expectedWanPublisherConfigIterator
                = expected.getBatchPublisherConfigs().iterator();
        Iterator<WanBatchReplicationPublisherConfig> actualWanPublisherConfigIterator
                = actual.getBatchPublisherConfigs().iterator();
        while (expectedWanPublisherConfigIterator.hasNext()) {
            assertWanPublisherConfig(expectedWanPublisherConfigIterator.next(), actualWanPublisherConfigIterator.next());
        }
        assertEquals(expected.toString(), actual.toString());
    }
}
