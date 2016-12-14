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
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanPublisherConfigTest {

    private WanPublisherConfig config = new WanPublisherConfig();

    @Test
    public void testSerialization() {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("key", "value");

        config.setGroupName("groupName");
        config.setQueueCapacity(500);
        config.setQueueFullBehavior(WANQueueFullBehavior.THROW_EXCEPTION);
        config.setProperties(properties);
        config.setClassName("className");
        config.setImplementation("implementation");

        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(config);
        WanPublisherConfig deserialized = serializationService.toObject(serialized);

        assertWanPublisherConfig(config, deserialized);
    }

    static void assertWanPublisherConfig(WanPublisherConfig expected, WanPublisherConfig actual) {
        assertEquals(expected.getGroupName(), actual.getGroupName());
        assertEquals(expected.getQueueCapacity(), actual.getQueueCapacity());
        assertEquals(expected.getQueueFullBehavior(), actual.getQueueFullBehavior());
        assertEquals(expected.getProperties(), actual.getProperties());
        assertEquals(expected.getClassName(), actual.getClassName());
        assertEquals(expected.getImplementation(), actual.getImplementation());
        assertEquals(expected.toString(), actual.toString());
    }
}
