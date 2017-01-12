package com.hazelcast.internal.management.dto;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanReplicationConfigDTOTest {

    @Test
    public void testSerialization() {
        Map<String, Comparable> properties = new HashMap<String, Comparable>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        WanPublisherConfig wanPublisherConfig = new WanPublisherConfig()
                .setGroupName("myGroupName")
                .setQueueCapacity(23)
                .setClassName("myClassName")
                .setQueueFullBehavior(WANQueueFullBehavior.THROW_EXCEPTION)
                .setProperties(properties);

        WanReplicationConfig expectedConfig = new WanReplicationConfig()
                .setName("myName")
                .addWanPublisherConfig(wanPublisherConfig);

        WanReplicationConfigDTO dto = new WanReplicationConfigDTO(expectedConfig);

        JsonObject json = dto.toJson();
        WanReplicationConfigDTO deserialized = new WanReplicationConfigDTO(null);
        deserialized.fromJson(json);

        WanReplicationConfig actualConfig = deserialized.getConfig();
        assertEquals(expectedConfig.getName(), actualConfig.getName());

        List<WanPublisherConfig> wanPublisherConfigs = actualConfig.getWanPublisherConfigs();
        assertEquals(1, wanPublisherConfigs.size());

        WanPublisherConfig actualWanPublisherConfig = wanPublisherConfigs.get(0);
        assertEquals(wanPublisherConfig.getGroupName(), actualWanPublisherConfig.getGroupName());
        assertEquals(wanPublisherConfig.getQueueCapacity(), actualWanPublisherConfig.getQueueCapacity());
        assertEquals(wanPublisherConfig.getClassName(), actualWanPublisherConfig.getClassName());
        assertEquals(wanPublisherConfig.getQueueFullBehavior(), actualWanPublisherConfig.getQueueFullBehavior());
        assertEquals(wanPublisherConfig.getProperties(), actualWanPublisherConfig.getProperties());
    }
}
