/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Rule to save and restore logging-related System properties.
 *
 * It's useful for tests poking test configuration.
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
