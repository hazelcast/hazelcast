/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CustomSpringJUnit4ClassRunner extends SpringJUnit4ClassRunner {

    static {
        TestLoggingUtils.initializeLogging();
        ClusterProperty.PHONE_HOME_ENABLED.setSystemProperty("false");
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
    protected Statement withBeforeClasses(Statement statement) {
        setProperties();
        return super.withBeforeClasses(statement);
    }

    @Override
    protected Statement withAfterClasses(Statement statement) {
        restoreProperties();
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

    /**
     * includes {@link com.hazelcast.test.HazelcastTestSupport#smallInstanceConfig}
     * except SqlConfig executor pool size, JetConfig cooperative thread count.
     */
    HashMap<String, String> propertiesMap = new HashMap<String, String>() {{
        put("java.net.preferIPv4Stack", "true");
        put("hazelcast.local.localAddress", "127.0.0.1");
        put(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "1");

        put(ClusterProperty.PARTITION_COUNT.getName(), "11");
        put(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "2");
        put(ClusterProperty.GENERIC_OPERATION_THREAD_COUNT.getName(), "2");
        put(ClusterProperty.EVENT_THREAD_COUNT.getName(), "1");
    }};

    private void setProperties() {
        for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
            String prevValue = System.getProperty(entry.getKey());
            System.setProperty(entry.getKey(), entry.getValue());
            entry.setValue(prevValue);
        }
    }

    private void restoreProperties() {
        for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
            if (entry.getValue() == null) {
                System.clearProperty(entry.getKey());
            } else {
                System.setProperty(entry.getKey(), entry.getValue());
            }
        }
    }
}
