package com.hazelcast.test.classhistogram;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * JUnit rule to dump class histogram on a test failure.
 *
 * It gives you an insight into a heap state. This can help
 * with reasoning about a test failure when you suspect the failure is related to
 * memory utilization.
 *
 * This is a best effort rule as it depends on number of JVM-specific tools/APIs.
 *
 * @see com.hazelcast.test.jitter.JitterRule
 *
 */
public class ClassHistogramRule implements TestRule {
    private static final int MAX_LINES_TO_DUMP = 100;

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    ClassHistogram.dump(MAX_LINES_TO_DUMP);
                    throw t;
                }
            }
        };
    }
}
