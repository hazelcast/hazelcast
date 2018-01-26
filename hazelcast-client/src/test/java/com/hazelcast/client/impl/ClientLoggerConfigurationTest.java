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

package com.hazelcast.client.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Log4j2Factory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.SaveLoggingPropertiesRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertSame;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class ClientLoggerConfigurationTest extends HazelcastTestSupport {

    private TestHazelcastFactory hazelcastFactory;

    @Rule
    public SaveLoggingPropertiesRule saveLoggingPropertiesRule = new SaveLoggingPropertiesRule();

    @Before
    public void setUp() {
        System.clearProperty("hazelcast.logging.type");
        System.clearProperty("hazelcast.logging.class");
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void testProgrammaticConfiguration() throws IOException {
        testLoggingWithConfiguration(true);
    }

    @Test
    public void testSystemPropertyConfiguration() throws IOException {
        testLoggingWithConfiguration(false);
    }

    // Test with programmatic or system property configuration according to boolean parameter.

    // the idea of the test is to configure a specific logging type for a client and then
    // test its LoggingService produce instances of the expected Logger impl
    protected void testLoggingWithConfiguration(boolean programmaticConfiguration) throws IOException {
        hazelcastFactory = new TestHazelcastFactory();
        Config cg = new Config();
        cg.setProperty("hazelcast.logging.type", "jdk");
        hazelcastFactory.newHazelcastInstance(cg);

        ClientConfig config = new ClientConfig();
        if (programmaticConfiguration) {
            config.setProperty("hazelcast.logging.type", "log4j2");
        } else {
            System.setProperty("hazelcast.logging.type", "log4j2");
        }
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(config);

        ILogger clientLogger = client.getLoggingService().getLogger("loggerName");
        // this part is fragile.
        // client wraps the actual logger in its own class
        ILogger actualLogger = (ILogger) getFromField(clientLogger, "logger");
        Class<?> clientLoggerClass = actualLogger.getClass();

        ILogger expectedLogger = new Log4j2Factory().getLogger("expectedLogger");
        Class<?> expectedLoggerClass = expectedLogger.getClass();

        assertSame(expectedLoggerClass, clientLoggerClass);
    }
}

