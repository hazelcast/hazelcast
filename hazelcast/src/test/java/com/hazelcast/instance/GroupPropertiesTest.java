package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link GroupProperty} and {@link GroupProperties} classes.
 * <p/>
 * Need to run with {@link HazelcastSerialClassRunner} due to tests with System environment variables.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class GroupPropertiesTest {

    private final Config config = new Config();
    private final GroupProperties defaultGroupProperties = new GroupProperties(config);

    @Test(expected = NullPointerException.class)
    public void constructor_withNullConfig() {
        new GroupProperties(null);
    }

    @Test
    public void setProperty_ensureHighestPriorityOfConfig() {
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, "configValue");
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty("systemValue");

        GroupProperties groupProperties = new GroupProperties(config);
        String value = groupProperties.getString(GroupProperty.ENTERPRISE_LICENSE_KEY);

        GroupProperty.ENTERPRISE_LICENSE_KEY.clearSystemProperty();

        assertEquals("configValue", value);
    }

    @Test
    public void setProperty_ensureUsageOfSystemProperty() {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty("systemValue");

        GroupProperties groupProperties = new GroupProperties(config);
        String value = groupProperties.getString(GroupProperty.ENTERPRISE_LICENSE_KEY);

        GroupProperty.ENTERPRISE_LICENSE_KEY.clearSystemProperty();

        assertEquals("systemValue", value);
    }

    @Test
    public void setProperty_ensureUsageOfDefaultValue() {
        String value = defaultGroupProperties.getString(GroupProperty.ENTERPRISE_LICENSE_KEY);

        assertNull(value);
    }

    @Test
    public void setProperty_inheritDefaultValueOfParentProperty() {
        String inputIOThreadCount = defaultGroupProperties.getString(GroupProperty.IO_INPUT_THREAD_COUNT);

        assertEquals(GroupProperty.IO_THREAD_COUNT.getDefaultValue(), inputIOThreadCount);
    }

    @Test
    public void setProperty_inheritActualValueOfParentProperty() {
        config.setProperty(GroupProperty.IO_THREAD_COUNT, "1");
        GroupProperties groupProperties = new GroupProperties(config);

        String inputIOThreadCount = groupProperties.getString(GroupProperty.IO_INPUT_THREAD_COUNT);

        assertEquals("1", inputIOThreadCount);
        assertNotEquals(GroupProperty.IO_THREAD_COUNT.getDefaultValue(), inputIOThreadCount);
    }

    @Test
    public void getSystemProperty() {
        GroupProperty.APPLICATION_VALIDATION_TOKEN.setSystemProperty("token");

        assertEquals("token", GroupProperty.APPLICATION_VALIDATION_TOKEN.getSystemProperty());

        GroupProperty.APPLICATION_VALIDATION_TOKEN.clearSystemProperty();
    }

    @Test
    public void getBoolean() {
        boolean isHumanReadable = defaultGroupProperties.getBoolean(GroupProperty.PERFORMANCE_MONITOR_HUMAN_FRIENDLY_FORMAT);

        assertTrue(isHumanReadable);
    }

    @Test
    public void getInteger() {
        int ioThreadCount = defaultGroupProperties.getInteger(GroupProperty.IO_THREAD_COUNT);

        assertEquals(3, ioThreadCount);
    }

    @Test
    public void getLong() {
        long lockMaxLeaseTimeSeconds = defaultGroupProperties.getLong(GroupProperty.LOCK_MAX_LEASE_TIME_SECONDS);

        assertEquals(Long.MAX_VALUE, lockMaxLeaseTimeSeconds);
    }

    @Test
    public void getFloat() {
        float maxFileSize = defaultGroupProperties.getFloat(GroupProperty.PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE_MB);

        assertEquals(10, maxFileSize, 0.0001);
    }

    @Test
    public void getTimeUnit() {
        config.setProperty(GroupProperty.PARTITION_TABLE_SEND_INTERVAL, "300");
        GroupProperties groupProperties = new GroupProperties(config);

        assertEquals(300, groupProperties.getSeconds(GroupProperty.PARTITION_TABLE_SEND_INTERVAL));
    }

    @Test
    public void getTimeUnit_default() {
        long expectedSeconds = 15;

        long intervalNanos = defaultGroupProperties.getNanos(GroupProperty.PARTITION_TABLE_SEND_INTERVAL);
        long intervalMillis = defaultGroupProperties.getMillis(GroupProperty.PARTITION_TABLE_SEND_INTERVAL);
        long intervalSeconds = defaultGroupProperties.getSeconds(GroupProperty.PARTITION_TABLE_SEND_INTERVAL);

        assertEquals(TimeUnit.SECONDS.toNanos(expectedSeconds), intervalNanos);
        assertEquals(TimeUnit.SECONDS.toMillis(expectedSeconds), intervalMillis);
        assertEquals(expectedSeconds, intervalSeconds);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getTimeUnit_noTimeUnitProperty() {
        defaultGroupProperties.getMillis(GroupProperty.APPLICATION_VALIDATION_TOKEN);
    }

    @Test
    public void getEnum() {
        config.setProperty(GroupProperty.PERFORMANCE_METRICS_LEVEL, ProbeLevel.DEBUG.toString());
        GroupProperties groupProperties = new GroupProperties(config);

        ProbeLevel level = groupProperties.getEnum(GroupProperty.PERFORMANCE_METRICS_LEVEL, ProbeLevel.class);

        assertEquals(ProbeLevel.DEBUG, level);
    }

    @Test
    public void getEnum_default() {
        ProbeLevel level = defaultGroupProperties.getEnum(GroupProperty.PERFORMANCE_METRICS_LEVEL, ProbeLevel.class);

        assertEquals(ProbeLevel.MANDATORY, level);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEnum_nonExistingEnum() {
        config.setProperty(GroupProperty.PERFORMANCE_METRICS_LEVEL, "notExist");
        GroupProperties groupProperties = new GroupProperties(config);
        groupProperties.getEnum(GroupProperty.PERFORMANCE_METRICS_LEVEL, ProbeLevel.class);
    }

    @Test
    public void getEnum_ignoredName() {
        config.setProperty(GroupProperty.PERFORMANCE_METRICS_LEVEL, "dEbUg");
        GroupProperties groupProperties = new GroupProperties(config);

        ProbeLevel level = groupProperties.getEnum(GroupProperty.PERFORMANCE_METRICS_LEVEL, ProbeLevel.class);

        assertEquals(ProbeLevel.DEBUG, level);
    }
}
