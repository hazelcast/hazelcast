package com.hazelcast.internal.dynamicconfig;

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
        //system properties are cleared automatically by the Hazelcast Runner
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, Versions.V3_8.toString());
        HazelcastInstance hazelcastInstance = createHazelcastInstance();

        hazelcastInstance.getConfig().addMapConfig(new MapConfig(randomName()));
    }


}
