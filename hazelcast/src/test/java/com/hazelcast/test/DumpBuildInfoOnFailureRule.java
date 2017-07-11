package com.hazelcast.test;

import com.hazelcast.instance.BuildInfoProvider;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class DumpBuildInfoOnFailureRule implements TestRule {
    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    printBuildInfo(description);
                    throw t;
                }
            }
        };
    }

    private void printBuildInfo(Description description) {
        System.out.println("BuildInfo right after " + description.getDisplayName() + ": "
                + BuildInfoProvider.getBuildInfo());
    }
}
