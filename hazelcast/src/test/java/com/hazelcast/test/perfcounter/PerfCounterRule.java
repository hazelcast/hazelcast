package com.hazelcast.test.perfcounter;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * JUnit rule to dump JVM perf counter on a test failure.
 *
 * This is a best effort rule as it depends on number of JVM-specific tools/APIs.
 *
 */
public class PerfCounterRule implements TestRule {
    private static final int MAX_LINES_TO_DUMP = 1000;

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    PerfCounter.dump(MAX_LINES_TO_DUMP);
                    throw t;
                }
            }
        };
    }
}
