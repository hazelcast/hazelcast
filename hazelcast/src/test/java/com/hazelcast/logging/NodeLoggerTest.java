package com.hazelcast.logging;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.IsolatedLoggingRule;
import org.junit.Test;

import static com.hazelcast.internal.TestSupport.assertInstanceOf;

public class NodeLoggerTest {

    @Test
    public void getLogger_whenTypeConfiguredForInstance_thenReturnLoggerOfConfiguredType() {
        final ILogger loggerBeforeInstanceStartup = Logger.getLogger(getClass());

        final Config config = new Config();
        config.setProperty(IsolatedLoggingRule.LOGGING_TYPE_PROPERTY, IsolatedLoggingRule.LOGGING_TYPE_LOG4J2);
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        try {
            final ILogger loggerAfterInstanceStartup = Logger.getLogger(getClass());

            assertInstanceOf(StandardLoggerFactory.StandardLogger.class, loggerBeforeInstanceStartup);
            assertInstanceOf(Log4j2Factory.Log4j2Logger.class, loggerAfterInstanceStartup);
        } finally {
            instance.shutdown();
        }
    }
}
