package com.hazelcast.internal.management.dto;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;
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
public class WanPublisherConfigDTOTest {

    @Test
    public void testSerialization() {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        WanPublisherConfig expected = new WanPublisherConfig()
                .setGroupName("myGroupName")
                .setQueueCapacity(23)
                .setClassName("myClassName")
                .setQueueFullBehavior(WANQueueFullBehavior.THROW_EXCEPTION)
                .setProperties(properties);

        WanPublisherConfigDTO dto = new WanPublisherConfigDTO(expected);

        JsonObject json = dto.toJson();
        WanPublisherConfigDTO deserialized = new WanPublisherConfigDTO(null);
        deserialized.fromJson(json);

        WanPublisherConfig actual = deserialized.getConfig();
        assertEquals(expected.getGroupName(), actual.getGroupName());
        assertEquals(expected.getQueueCapacity(), actual.getQueueCapacity());
        assertEquals(expected.getClassName(), actual.getClassName());
        assertEquals(expected.getQueueFullBehavior(), actual.getQueueFullBehavior());
        assertEquals(expected.getProperties(), actual.getProperties());
    }
}
