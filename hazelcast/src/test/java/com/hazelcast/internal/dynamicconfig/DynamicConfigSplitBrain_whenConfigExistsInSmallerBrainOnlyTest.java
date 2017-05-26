package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.TestConfigUtils;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DynamicConfigSplitBrain_whenConfigExistsInSmallerBrainOnlyTest extends SplitBrainTestSupport {

    private static final String MAP_NAME = "mapConfigCreatedInSmallerBrain";

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) throws Exception {
        HazelcastInstance instanceInSmallerBrain = firstBrain[0];
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setInMemoryFormat(TestConfigUtils.NON_DEFAULT_IN_MEMORY_FORMAT);
        mapConfig.setBackupCount(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT);
        instanceInSmallerBrain.getConfig().addMapConfig(mapConfig);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        for (HazelcastInstance instance : instances) {
            final Config config = instance.getConfig();
            MapConfig mapConfig = config.findMapConfig(MAP_NAME);
            assertEquals(TestConfigUtils.NON_DEFAULT_IN_MEMORY_FORMAT, mapConfig.getInMemoryFormat());
            assertEquals(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT, mapConfig.getBackupCount());
        }
    }
}
