package com.hazelcast.osgi.impl;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.osgi.TestBundle;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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

    protected static String loggingType;
    protected static Level log4jLogLevel;

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

    @BeforeClass
    public static void beforeClass() {
        loggingType = System.getProperty("hazelcast.logging.type");
        Logger rootLogger = Logger.getRootLogger();
        if ("log4j".equals(loggingType)) {
            log4jLogLevel = rootLogger.getLevel();
        }
        rootLogger.setLevel(Level.TRACE);
        System.setProperty("hazelcast.logging.type", "log4j");
    }

    @AfterClass
    public static void afterClass() {
        if (loggingType != null) {
            System.setProperty("hazelcast.logging.type", loggingType);
        }
        if (log4jLogLevel != null) {
            Logger.getRootLogger().setLevel(log4jLogLevel);
        }
    }

    @Before
    public void setup() throws BundleException {
        bundle = new TestBundle();
        bundle.start();
    }

    @After
    public void tearDown() throws BundleException {
        try  {
            bundle.stop();
            bundle = null;
        } finally {
            Hazelcast.shutdownAll();
        }
    }

}
