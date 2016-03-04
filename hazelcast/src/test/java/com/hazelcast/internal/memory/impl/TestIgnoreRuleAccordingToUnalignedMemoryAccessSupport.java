package com.hazelcast.internal.memory.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class TestIgnoreRuleAccordingToUnalignedMemoryAccessSupport implements TestRule {

    private static final ILogger LOGGER
            = Logger.getLogger(TestIgnoreRuleAccordingToUnalignedMemoryAccessSupport.class);

    @Override
    public Statement apply(Statement base, final Description description) {
        if (description.getAnnotation(RequiresUnalignedMemoryAccessSupport.class) != null
            && !AlignmentUtil.isUnalignedAccessAllowed()) {
                return new Statement() {
                    @Override
                    public void evaluate() throws Throwable {
                        LOGGER.finest("Ignoring `" + description.getClassName()
                                + "` because unaligned memory access is not supported in this platform");
                    }
                };
        }
        return base;
    }

}
