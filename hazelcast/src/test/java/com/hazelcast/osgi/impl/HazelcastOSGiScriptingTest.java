package com.hazelcast.osgi.impl;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.osgi.TestBundle;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.model.Statement;
import org.osgi.framework.BundleException;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public abstract class HazelcastOSGiScriptingTest {

    protected TestBundle bundle;

    @Rule
    public TestRule scriptingAvailableRule = new TestRule() {

        @Override
        public Statement apply(Statement base, Description description) {
            if (Activator.isJavaxScriptingAvailable()) {
                return base;
            } else {
                return new Statement() {
                    @Override
                    public void evaluate() throws Throwable {
                        System.out.println("Ignoring test since scripting is not available!");
                    }
                };
            }
        }

    };

    @Before
    public void setup() throws BundleException {
        bundle = new TestBundle();
        bundle.start();
    }

    @After
    public void tearDown() throws BundleException {
        try {
            bundle.stop();
            bundle = null;
        } finally {
            Hazelcast.shutdownAll();
        }
    }

}
