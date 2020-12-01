package com.hazelcast.logging;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RemoveLoggerTest extends HazelcastTestSupport {

    LoggingService loggingService;

    @Before
    public void setup() {
        loggingService = createHazelcastInstance().getLoggingService();
    }

    @Test
    public void removeLogger() {
        // given
        ILogger logger = loggingService.getLogger(getClass());

        // when
        loggingService.reoveLogger(getClass());

        // then
        ILogger afterRemoveLogger = loggingService.getLogger(getClass());
        assertNotEquals(logger, afterRemoveLogger);
    }
}
