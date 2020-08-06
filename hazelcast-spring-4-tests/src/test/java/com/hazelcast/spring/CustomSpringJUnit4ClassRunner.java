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

package com.hazelcast.spring;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.JmxLeakHelper;
import com.hazelcast.test.TestLoggingUtils;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Set;

public class CustomSpringJUnit4ClassRunner extends SpringJUnit4ClassRunner {

    static {
        TestLoggingUtils.initializeLogging();
        System.setProperty("java.net.preferIPv4Stack", "true");
        ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.setSystemProperty("1");
        ClusterProperty.PHONE_HOME_ENABLED.setSystemProperty("false");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
    }

    /**
     * Constructs a new <code>SpringJUnit4ClassRunner</code> and initializes a
     * {@link org.springframework.test.context.TestContextManager} to provide Spring testing functionality to
     * standard JUnit tests.
     *
     * @param clazz the test class to be run
     * @see #createTestContextManager(Class)
     */
    public CustomSpringJUnit4ClassRunner(Class<?> clazz) throws InitializationError {
        super(clazz);
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        String testName = testName(method);
        TestLoggingUtils.setThreadLocalTestMethodName(testName);
        try {
            super.runChild(method, notifier);
        } finally {
            TestLoggingUtils.removeThreadLocalTestMethodName();
        }
    }

    @Override
    protected Statement withAfterClasses(Statement statement) {
        final Statement originalStatement = super.withAfterClasses(statement);
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                originalStatement.evaluate();

                Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
                if (!instances.isEmpty()) {
                    String message = "Instances haven't been shut down: " + instances;
                    Hazelcast.shutdownAll();
                    throw new IllegalStateException(message);
                }

                JmxLeakHelper.checkJmxBeans();
            }
        };
    }
}
