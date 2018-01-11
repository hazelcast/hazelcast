package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.TestConfigUtils;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DynamicConfigSplitBrain_whenDifferentConfigExistsInBothBrainsTest extends SplitBrainTestSupport {

    private static final String MAP_NAME = "mapConfigWithNonDefaultName";

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        HazelcastInstance instanceInBiggerBrain = firstBrain[0];
        MapConfig defaultMapConfig = new MapConfig(MAP_NAME);
        instanceInBiggerBrain.getConfig().addMapConfig(defaultMapConfig);

        HazelcastInstance instanceInSmallerBrain = secondBrain[0];
        MapConfig mapConfig = new MapConfig(MAP_NAME)
                .setInMemoryFormat(TestConfigUtils.NON_DEFAULT_IN_MEMORY_FORMAT)
                .setBackupCount(TestConfigUtils.NON_DEFAULT_BACKUP_COUNT);
        instanceInSmallerBrain.getConfig().addMapConfig(mapConfig);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        MapConfig defaultMapConfig = new Config().findMapConfig("default");

        for (HazelcastInstance instance : instances) {
            MapConfig mapConfig = instance.getConfig().findMapConfig(MAP_NAME);
            assertEquals(defaultMapConfig.getInMemoryFormat(), mapConfig.getInMemoryFormat());
            assertEquals(defaultMapConfig.getBackupCount(), mapConfig.getBackupCount());
        }
    }
}
