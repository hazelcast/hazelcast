package com.hazelcast.internal.memory.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class TestIgnoreRuleAccordingToUnsafeAvailability implements TestRule {

    private static final ILogger LOGGER = Logger.getLogger(TestIgnoreRuleAccordingToUnsafeAvailability.class);

    @Override
    public Statement apply(Statement base, final Description description) {
        if (UnsafeUtil.UNSAFE_AVAILABLE) {
            return base;
        } else {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    LOGGER.finest("Ignoring `" + description.getClassName() + "` because Unsafe is not available");
                }
            };
        }
    }

}
