package com.hazelcast.logging;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.SaveLoggingPropertiesRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link com.hazelcast.logging.Logger} class.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LoggerTest extends HazelcastTestSupport {

    private static final String LOGGING_TYPE_PROPERTY_NAME = "hazelcast.logging.type";
    private static final String LOGGING_CLASS_PROPERTY_NAME = "hazelcast.logging.class";
    private static final String LOGGING_TYPE_LOG4J = "log4j";
    private static final String LOGGING_TYPE_LOG4J2 = "log4j2";

    private static Field LOGGER_FACTORY_FIELD;

    @Rule
    public SaveLoggingPropertiesRule saveLoggingPropertiesRule = new SaveLoggingPropertiesRule();

    @BeforeClass
    public static void beforeClass() {
        try {
            LOGGER_FACTORY_FIELD = Logger.class.getDeclaredField("loggerFactory");
            LOGGER_FACTORY_FIELD.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(
                    "Couldn't retrieve \"loggerFactory\" field from " + Logger.class.getName() + " class !", e);
        }
    }

    @Before
    public void before() {
        try {
            // Reset logger factory field
            LOGGER_FACTORY_FIELD.set(null, null);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                    "Couldn't clear \"loggerFactory\" field from " + Logger.class.getName() + " class !", e);
        }
        System.clearProperty(LOGGING_CLASS_PROPERTY_NAME);
    }

    @Test
    public void testLog4jLogger() {
        System.setProperty(LOGGING_TYPE_PROPERTY_NAME, LOGGING_TYPE_LOG4J);
        assertEquals(Logger.getLogger(getClass()).getClass(), Log4jFactory.Log4jLogger.class);
    }

    @Test
    public void testLog4j2Logger() {
        System.setProperty(LOGGING_TYPE_PROPERTY_NAME, LOGGING_TYPE_LOG4J2);
        assertEquals(Logger.getLogger(getClass()).getClass(), Log4j2Factory.Log4j2Logger.class);
    }

}
