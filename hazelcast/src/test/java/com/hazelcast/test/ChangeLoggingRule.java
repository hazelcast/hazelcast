/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggerFactory;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Test rule that allows to change logging setting on-the-fly for a single test.
 * Example of usage, add this to your test class:
 *
 * <pre>
 * @ClassRule
 * public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-debug.xml");
 * </pre>
 *
 * See log4j2-debug.xml in test resources for example.
 *
 */
public class ChangeLoggingRule implements TestRule {

    private final String configFile;

    public ChangeLoggingRule(String configFile) {
        this.configFile = configFile;
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                LoggerFactory loggerFactory = Logger.newLoggerFactory(null);
                if (loggerFactory instanceof TestLoggerFactory) {
                    TestLoggerFactory testLoggerFactory = (TestLoggerFactory) loggerFactory;
                    testLoggerFactory.changeConfigFile(configFile); //setting the desired configuration
                    try {
                        base.evaluate();
                    } finally {
                        testLoggerFactory.changeConfigFile(null); //resetting to default behavior
                    }
                } else {
                    System.out.printf("ChangeLoggingRule: could not change config file to '%s' because "
                            + "logger factory is instance of '%s' instead of expected TestLoggerFactory.\n",
                            configFile, loggerFactory.getClass());
                    base.evaluate();
                }
            }
        };
    }
}
