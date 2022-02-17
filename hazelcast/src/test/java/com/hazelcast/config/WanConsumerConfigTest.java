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

package com.hazelcast.config;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
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
public class WanConsumerConfigTest {

    private WanConsumerConfig config = new WanConsumerConfig();

    @Test
    public void testSerialization() {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("key", "value");

        config.setProperties(properties);
        config.setClassName("className");
        config.setImplementation(new DummyWanConsumer());
        config.setPersistWanReplicatedData(false);

        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(config);
        WanConsumerConfig deserialized = serializationService.toObject(serialized);

        assertWanConsumerConfig(config, deserialized);
    }

    static void assertWanConsumerConfig(WanConsumerConfig expected, WanConsumerConfig actual) {
        if (expected == null) {
            return;
        }
        assertEquals(expected.getProperties(), actual.getProperties());
        assertEquals(expected.getClassName(), actual.getClassName());
        assertEquals(expected.getImplementation(), actual.getImplementation());
        assertEquals(expected.isPersistWanReplicatedData(), actual.isPersistWanReplicatedData());
        assertEquals(expected.toString(), actual.toString());
    }
}
