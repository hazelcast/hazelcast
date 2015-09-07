package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class GroupPropertiesTest {

    @Test
    public void getEnum_default() {
        GroupProperties groupProperties = new GroupProperties(new Config());

        ProbeLevel level = groupProperties.getEnum(GroupProperty.PERFORMANCE_METRICS_LEVEL, ProbeLevel.class);

        assertEquals(ProbeLevel.MANDATORY, level);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEnum_nonExistingEnum() {
        Config config = new Config();
        config.setProperty(GroupProperty.PERFORMANCE_METRICS_LEVEL, "notexist");
        GroupProperties groupProperties = new GroupProperties(config);
        groupProperties.getEnum(GroupProperty.PERFORMANCE_METRICS_LEVEL, ProbeLevel.class);

    }

    @Test
    public void getEnum() {
        Config config = new Config();
        config.setProperty(GroupProperty.PERFORMANCE_METRICS_LEVEL, ProbeLevel.DEBUG.toString());
        GroupProperties groupProperties = new GroupProperties(config);

        ProbeLevel level = groupProperties.getEnum(GroupProperty.PERFORMANCE_METRICS_LEVEL, ProbeLevel.class);

        assertEquals(ProbeLevel.DEBUG, level);
    }

    @Test
    public void getEnum_ignoredName() {
        Config config = new Config();
        config.setProperty(GroupProperty.PERFORMANCE_METRICS_LEVEL, "dEbUg");
        GroupProperties groupProperties = new GroupProperties(config);

        ProbeLevel level = groupProperties.getEnum(GroupProperty.PERFORMANCE_METRICS_LEVEL, ProbeLevel.class);

        assertEquals(ProbeLevel.DEBUG, level);
    }
}
