package com.hazelcast.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Rule to save and restore logging-related System properties.
 *
 * It's useful for tests poking test configuration.
 *
 */
public class SaveLoggingPropertiesRule implements TestRule {

    private static final String LOGGING_TYPE_PROP_NAME = "hazelcast.logging.type";
    private static final String LOGGING_CLASS_PROP_NAME = "hazelcast.logging.class";

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                String oldLoggingType = System.getProperty(LOGGING_TYPE_PROP_NAME);
                String oldLoggingClass = System.getProperty(LOGGING_CLASS_PROP_NAME);
                try {
                    base.evaluate();
                } finally {
                    setOrClearProperty(LOGGING_TYPE_PROP_NAME, oldLoggingType);
                    setOrClearProperty(LOGGING_CLASS_PROP_NAME, oldLoggingClass);
                }
            }

            private void setOrClearProperty(String propertyName, String value) {
                if (value == null) {
                    System.clearProperty(propertyName);
                } else {
                    System.setProperty(propertyName, value);
                }
            }
        };
    }
}
