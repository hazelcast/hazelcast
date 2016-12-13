package com.hazelcast.config;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.config.WanConsumerConfigTest.assertWanConsumerConfig;
import static com.hazelcast.config.WanPublisherConfigTest.assertWanPublisherConfig;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanReplicationConfigTest {

    private WanReplicationConfig config = new WanReplicationConfig();

    @Test
    public void testSerialization() {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("key", "value");

        WanConsumerConfig wanConsumerConfig = new WanConsumerConfig();
        wanConsumerConfig.setProperties(properties);
        wanConsumerConfig.setClassName("className");
        wanConsumerConfig.setImplementation("implementation");

        WanPublisherConfig wanPublisherConfig1 = new WanPublisherConfig();
        WanPublisherConfig wanPublisherConfig2 = new WanPublisherConfig();

        List<WanPublisherConfig> publisherConfigs = new LinkedList<WanPublisherConfig>();
        publisherConfigs.add(wanPublisherConfig1);

        config.setName("name");
        config.setWanConsumerConfig(wanConsumerConfig);
        config.setWanPublisherConfigs(publisherConfigs);
        config.addWanPublisherConfig(wanPublisherConfig2);

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
        Iterator<WanPublisherConfig> expectedWanPublisherConfigIterator = expected.getWanPublisherConfigs().iterator();
        Iterator<WanPublisherConfig> actualWanPublisherConfigIterator = actual.getWanPublisherConfigs().iterator();
        while (expectedWanPublisherConfigIterator.hasNext()) {
            assertWanPublisherConfig(expectedWanPublisherConfigIterator.next(), actualWanPublisherConfigIterator.next());
        }
        assertEquals(expected.toString(), actual.toString());
    }
}
