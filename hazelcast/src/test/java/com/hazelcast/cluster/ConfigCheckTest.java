package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ConfigCheckTest {

    @Test
    public void whenGroupNameDifferent_thenFalse() {
        Config config1 = new Config();
        config1.getGroupConfig().setName("foo");

        Config config2 = new Config();
        config2.getGroupConfig().setName("bar");

        ConfigCheck configCheck1 = new ConfigCheck(config1, "joiner");
        ConfigCheck configCheck2 = new ConfigCheck(config2, "joiner");

        assertIsCompatibleFalse(configCheck1, configCheck2);
    }

    @Test
    public void whenGroupNameSameButPasswordDifferent_thenConfigMismatchException() {
        Config config1 = new Config();
        config1.getGroupConfig().setName("group").setPassword("password1");
        Config config2 = new Config();
        config2.getGroupConfig().setName("group").setPassword("password2");

        ConfigCheck configCheck1 = new ConfigCheck(config1, "joiner");
        ConfigCheck configCheck2 = new ConfigCheck(config2, "joiner");

        assertIsCompatibleThrowsConfigMismatchException(configCheck1, configCheck2);
    }

    @Test
    public void whenJoinerTypeDifferent_thenConfigMismatchException() {
        Config config1 = new Config();
        Config config2 = new Config();

        ConfigCheck configCheck1 = new ConfigCheck(config1, "joiner1");
        ConfigCheck configCheck2 = new ConfigCheck(config2, "joiner2");

        assertIsCompatibleThrowsConfigMismatchException(configCheck1, configCheck2);
    }

    @Test
    public void whenDifferentPartitionCount_thenConfigationMismatchException() {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_PARTITION_COUNT, "100");

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_PARTITION_COUNT, "200");

        ConfigCheck configCheck1 = new ConfigCheck(config1, "joiner");
        ConfigCheck configCheck2 = new ConfigCheck(config2, "joiner");

        assertIsCompatibleThrowsConfigMismatchException(configCheck1, configCheck2);
    }

    @Test
    public void whenDifferentApplicationValidationToken_thenConfigurationMismatchException() {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_APPLICATION_VALIDATION_TOKEN, "foo");

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_APPLICATION_VALIDATION_TOKEN, "bar");

        ConfigCheck configCheck1 = new ConfigCheck(config1, "joiner");
        ConfigCheck configCheck2 = new ConfigCheck(config2, "joiner");

        assertIsCompatibleThrowsConfigMismatchException(configCheck1, configCheck2);
    }

    @Test
    public void whenGroupPartitionEnabledMismatch_thenConfigurationMismatchException() {
        Config config1 = new Config();
        config1.getPartitionGroupConfig().setEnabled(false);

        Config config2 = new Config();
        config2.getPartitionGroupConfig().setEnabled(true);

        ConfigCheck configCheck1 = new ConfigCheck(config1, "joiner");
        ConfigCheck configCheck2 = new ConfigCheck(config2, "joiner");

        assertIsCompatibleThrowsConfigMismatchException(configCheck1, configCheck2);
    }

    @Test
    public void whenPartitionGroupGroupTypeMismatch_thenConfigurationMismatchException() {
        Config config1 = new Config();
        config1.getPartitionGroupConfig().setEnabled(true);
        config1.getPartitionGroupConfig().setGroupType(PartitionGroupConfig.MemberGroupType.CUSTOM);

        Config config2 = new Config();
        config2.getPartitionGroupConfig().setEnabled(true);
        config2.getPartitionGroupConfig().setGroupType(PartitionGroupConfig.MemberGroupType.PER_MEMBER);

        ConfigCheck configCheck1 = new ConfigCheck(config1, "joiner");
        ConfigCheck configCheck2 = new ConfigCheck(config2, "joiner");

        assertIsCompatibleThrowsConfigMismatchException(configCheck1, configCheck2);
    }


    public void assertIsCompatibleFalse(ConfigCheck c1, ConfigCheck c2) {
        Assert.assertFalse(c1.isCompatible(c2));
        Assert.assertFalse(c2.isCompatible(c1));
        assertIsCompatibleTrue(c1, c2);
    }

    public void assertIsCompatibleThrowsConfigMismatchException(ConfigCheck c1, ConfigCheck c2) {
        try {
            c1.isCompatible(c2);
            fail();
        } catch (ConfigMismatchException e) {

        }

        try {
            c2.isCompatible(c1);
            fail();
        } catch (ConfigMismatchException e) {

        }

        assertIsCompatibleTrue(c1, c2);
    }

    private void assertIsCompatibleTrue(ConfigCheck c1, ConfigCheck c2) {
        Assert.assertTrue(c1.isCompatible(c1));
        Assert.assertTrue(c2.isCompatible(c2));
    }
}
