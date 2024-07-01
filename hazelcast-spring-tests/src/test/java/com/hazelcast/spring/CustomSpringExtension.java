/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spring.config.ConfigFactoryAccessor;
import com.hazelcast.test.JmxLeakHelper;
import com.hazelcast.test.TestLoggingUtils;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;

/**
 * This Junit5 extension is used per test class. It is similar to a JUnit4 test class runner
 */
public class CustomSpringExtension implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback, InvocationInterceptor {

    // Specifies the default time out value for a test method, unless a test timeout annotation is not provided
    private static final int DEFAULT_TEST_TIMEOUT_IN_SECONDS = Integer.getInteger("hazelcast.test.defaultTestTimeoutInSeconds", 300);

    private static final int MAX_CLIENT_CONNECT_TIMEOUT_MS = 30_000;

    static {
        TestLoggingUtils.initializeLogging();
        ConfigFactoryAccessor.setConfigSupplier(() -> {
            Config config = smallInstanceConfig();
            config.setProperty("java.net.preferIPv4Stack", "true");
            config.setProperty("hazelcast.local.localAddress", "127.0.0.1");
            config.setProperty(ClusterProperty.PHONE_HOME_ENABLED.getName(), "false");
            config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "1");
            return config;
        });
        ConfigFactoryAccessor.setClientConfigSupplier(() -> {
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig()
                    .setClusterConnectTimeoutMillis(MAX_CLIENT_CONNECT_TIMEOUT_MS);
            return clientConfig;
        });
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        // Code to run before all tests
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        Method testMethod = context.getRequiredTestMethod();
        String testName = testMethod.getName();
        TestLoggingUtils.setThreadLocalTestMethodName(testName);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        TestLoggingUtils.removeThreadLocalTestMethodName();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
        if (!instances.isEmpty()) {
            String message = "Instances haven't been shut down: " + instances;
            Hazelcast.shutdownAll();
            throw new IllegalStateException(message);
        }
        JmxLeakHelper.checkJmxBeans();
    }

    @Override
    public void interceptTestMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) throws Throwable {
        Method testMethod = invocationContext.getExecutable();
        long finalTimeoutSeconds = getTimeoutSeconds(testMethod);
        Thread thread = new Thread(() -> {
            try {
                invocation.proceed();
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        });
        thread.setDaemon(true);
        thread.start();
        thread.join(finalTimeoutSeconds * 1000);
        if (thread.isAlive()) {
            thread.interrupt();
            throw new IllegalStateException("Test method " + testMethod.getName() + " timed out after " + finalTimeoutSeconds + " seconds");
        }
    }

    private long getTimeoutSeconds(Method testMethod) {
        Timeout timeout = testMethod.getAnnotation(Timeout.class);
        long finalTimeoutSeconds;
        if (timeout == null) {
            finalTimeoutSeconds = DEFAULT_TEST_TIMEOUT_IN_SECONDS;
        } else {
            finalTimeoutSeconds = timeout.value();
            if (timeout.unit() != TimeUnit.SECONDS) {
                finalTimeoutSeconds = timeout.unit().toSeconds(timeout.value());
            }
        }
        return finalTimeoutSeconds;
    }
}
