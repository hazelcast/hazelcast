package com.hazelcast.internal.metrics;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ProbeLevelTest {

    @Test
    public void isEnabled() {
        assertTrue(MANDATORY.isEnabled(MANDATORY));
        assertTrue(MANDATORY.isEnabled(INFO));
        assertTrue(MANDATORY.isEnabled(DEBUG));

        assertFalse(INFO.isEnabled(MANDATORY));
        assertTrue(INFO.isEnabled(INFO));
        assertTrue(INFO.isEnabled(DEBUG));

        assertFalse(DEBUG.isEnabled(MANDATORY));
        assertFalse(DEBUG.isEnabled(INFO));
        assertTrue(DEBUG.isEnabled(DEBUG));
    }
}
