package com.hazelcast.client.config;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ClientProperty} and {@link ClientProperties} classes.
 * <p/>
 * Need to run with {@link HazelcastSerialClassRunner} due to tests with System environment variables.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)

public class ClientPropertiesTest {

    private final ClientConfig config = new ClientConfig();
    private final ClientProperties defaultClientProperties = new ClientProperties(config);

    @Test(expected = NullPointerException.class)
    public void constructor_withNullConfig() {
        new ClientProperties(null);
    }

    @Test
    public void setProperty_ensureHighestPriorityOfConfig() {
        config.setProperty(ClientProperty.EVENT_THREAD_COUNT, "1000");
        ClientProperty.EVENT_THREAD_COUNT.setSystemProperty("5000");

        ClientProperties ClientProperties = new ClientProperties(config);
        int eventThreadCount = ClientProperties.getInteger(ClientProperty.EVENT_THREAD_COUNT);

        ClientProperty.EVENT_THREAD_COUNT.clearSystemProperty();

        assertEquals(1000, eventThreadCount);
    }

    @Test
    public void setProperty_ensureUsageOfSystemProperty() {
        ClientProperty.EVENT_THREAD_COUNT.setSystemProperty("2342");

        ClientProperties ClientProperties = new ClientProperties(config);
        int eventThreadCount = ClientProperties.getInteger(ClientProperty.EVENT_THREAD_COUNT);

        ClientProperty.EVENT_THREAD_COUNT.clearSystemProperty();

        assertEquals(2342, eventThreadCount);
    }

    @Test
    public void setProperty_ensureUsageOfDefaultValue() {
        int eventThreadCount = defaultClientProperties.getInteger(ClientProperty.EVENT_THREAD_COUNT);

        assertEquals(5, eventThreadCount);
    }

    @Test
    public void getSystemProperty() {
        ClientProperty.EVENT_THREAD_COUNT.setSystemProperty("8000");

        assertEquals("8000", ClientProperty.EVENT_THREAD_COUNT.getSystemProperty());

        ClientProperty.EVENT_THREAD_COUNT.clearSystemProperty();
    }

    @Test
    public void getBoolean() {
        boolean shuffleMemberList = defaultClientProperties.getBoolean(ClientProperty.SHUFFLE_MEMBER_LIST);

        assertTrue(shuffleMemberList);
    }

    @Test
    public void getInteger() {
        int eventThreadCount = defaultClientProperties.getInteger(ClientProperty.EVENT_THREAD_COUNT);

        assertEquals(5, eventThreadCount);
    }

    @Test
    public void getTimeUnit() {
        config.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS, "300");
        ClientProperties ClientProperties = new ClientProperties(config);

        assertEquals(300, ClientProperties.getSeconds(ClientProperty.INVOCATION_TIMEOUT_SECONDS));
    }

    @Test
    public void getTimeUnit_default() {
        long expectedSeconds = 5;

        long intervalNanos = defaultClientProperties.getNanos(ClientProperty.HEARTBEAT_INTERVAL);
        long intervalMillis = defaultClientProperties.getMillis(ClientProperty.HEARTBEAT_INTERVAL);
        long intervalSeconds = defaultClientProperties.getSeconds(ClientProperty.HEARTBEAT_INTERVAL);

        assertEquals(TimeUnit.SECONDS.toNanos(expectedSeconds), intervalNanos);
        assertEquals(TimeUnit.SECONDS.toMillis(expectedSeconds), intervalMillis);
        assertEquals(expectedSeconds, intervalSeconds);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getTimeUnit_noTimeUnitProperty() {
        defaultClientProperties.getMillis(ClientProperty.EVENT_QUEUE_CAPACITY);
    }
}
