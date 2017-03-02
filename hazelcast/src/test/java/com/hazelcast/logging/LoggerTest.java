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
    private static final String LOGGING_TYPE_SLF4J = "slf4j";
    private static final String LOGGING_TYPE_JDK = "jdk";
    private static final String LOGGING_TYPE_NONE = "none";

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
            // reset logger factory field
            LOGGER_FACTORY_FIELD.set(null, null);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                    "Couldn't clear \"loggerFactory\" field from " + Logger.class.getName() + " class !", e);
        }
        System.clearProperty(LOGGING_CLASS_PROPERTY_NAME);
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(Logger.class);
    }

    @Test
    public void getLogger_thenLog4j_thenReturnLog4jLogger() {
        System.setProperty(LOGGING_TYPE_PROPERTY_NAME, LOGGING_TYPE_LOG4J);
        assertInstanceOf(Log4jFactory.Log4jLogger.class, Logger.getLogger(getClass()));
    }

    @Test
    public void getLogger_thenLog4j2_thenReturnLog4j2Logger() {
        System.setProperty(LOGGING_TYPE_PROPERTY_NAME, LOGGING_TYPE_LOG4J2);
        assertInstanceOf(Log4j2Factory.Log4j2Logger.class, Logger.getLogger(getClass()));
    }

    @Test
    public void getLogger_whenSlf4j_thenReturnSlf4jLogger() {
        System.setProperty(LOGGING_TYPE_PROPERTY_NAME, LOGGING_TYPE_SLF4J);
        assertInstanceOf(Slf4jFactory.Slf4jLogger.class, Logger.getLogger(getClass()));
    }

    @Test
    public void getLogger_whenJdk_thenReturnStandardLogger() {
        System.setProperty(LOGGING_TYPE_PROPERTY_NAME, LOGGING_TYPE_JDK);
        assertInstanceOf(StandardLoggerFactory.StandardLogger.class, Logger.getLogger(getClass()));
    }

    @Test
    public void getLogger_whenNone_thenReturnNoLogger() {
        System.setProperty(LOGGING_TYPE_PROPERTY_NAME, LOGGING_TYPE_NONE);
        assertInstanceOf(NoLogFactory.NoLogger.class, Logger.getLogger(getClass()));
    }

    @Test
    public void getLogger_whenInvalidConfiguration_thenCreateStandardLogger() {
        System.setProperty(LOGGING_TYPE_PROPERTY_NAME, "invalid");
        assertInstanceOf(StandardLoggerFactory.StandardLogger.class, Logger.getLogger(getClass()));
    }

    @Test
    public void noLogger() {
        assertInstanceOf(NoLogFactory.NoLogger.class, Logger.noLogger());
    }
}
