package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.CountDownLatchConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DynamicConfigRollingUpgradeTest extends HazelcastTestSupport {

    @Test(expected = UnsupportedOperationException.class)
    public void testThrowsExceptionWhenRunningInClusterVersion38() {
        // system properties are cleared automatically by the Hazelcast Runner
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, Versions.V3_8.toString());
        HazelcastInstance hazelcastInstance = createHazelcastInstance();

        hazelcastInstance.getConfig().addMapConfig(new MapConfig(randomName()));
    }

    @Test(expected = ConfigurationException.class)
    public void testThrowsExceptionWhenAddingAtomicLongConfigInClusterVersion39() {
        // system properties are cleared automatically by the Hazelcast Runner
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, Versions.V3_9.toString());
        HazelcastInstance hazelcastInstance = createHazelcastInstance();

        hazelcastInstance.getConfig().addAtomicLongConfig(new AtomicLongConfig(randomMapName()));
    }

    @Test(expected = ConfigurationException.class)
    public void testThrowsExceptionWhenAddingAtomicReferenceConfigInClusterVersion39() {
        // system properties are cleared automatically by the Hazelcast Runner
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, Versions.V3_9.toString());
        HazelcastInstance hazelcastInstance = createHazelcastInstance();

        hazelcastInstance.getConfig().addAtomicReferenceConfig(new AtomicReferenceConfig(randomMapName()));
    }

    @Test(expected = ConfigurationException.class)
    public void testThrowsExceptionWhenAddingCountDownLatchConfigInClusterVersion39() {
        // system properties are cleared automatically by the Hazelcast Runner
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, Versions.V3_9.toString());
        HazelcastInstance hazelcastInstance = createHazelcastInstance();

        hazelcastInstance.getConfig().addCountDownLatchConfig(new CountDownLatchConfig(randomMapName()));
    }

}
