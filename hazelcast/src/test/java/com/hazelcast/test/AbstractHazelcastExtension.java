/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.test.annotation.QuickTest;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.AssumptionViolatedException;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.opentest4j.TestAbortedException;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;

import static com.hazelcast.cache.jsr.JsrTestUtil.clearCachingProviderRegistry;
import static com.hazelcast.cache.jsr.JsrTestUtil.getCachingProviderRegistrySize;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.TestEnvironment.isRunningCompatibilityTest;

/**
 * Abstract base JUnit 5 extension providing the same functionality as
 * {@link AbstractHazelcastClassRunner}, including:
 * <ul>
 *   <li>Hazelcast test environment initialization (system properties, logging)</li>
 *   <li>Thread-local test method name tracking</li>
 *   <li>Test timing and logging</li>
 *   <li>Post-class checks: no leftover Hazelcast instances, JMX beans, or CachingProviders</li>
 * </ul>
 * <p>
 * Subclasses provide system-property isolation strategies:
 * {@link HazelcastSerialTestExtension} uses per-test copy-on-write properties,
 * while {@link HazelcastParallelTestExtension} uses per-thread properties.
 */
public abstract class AbstractHazelcastExtension
        implements BeforeEachCallback, AfterEachCallback, AfterAllCallback, InvocationInterceptor {

    private static final boolean THREAD_DUMP_ON_FAILURE;
    private static final boolean THREAD_CPU_TIME_INFO_AVAILABLE;
    private static final boolean THREAD_CONTENTION_INFO_AVAILABLE;

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(AbstractHazelcastExtension.class);
    private static final String START_TIME_KEY = "startTime";

    static final ThreadLocal<String> TEST_NAME_THREAD_LOCAL = new InheritableThreadLocal<>();

    static {
        initialize();

        final String threadDumpOnFailure = System.getProperty("hazelcast.test.threadDumpOnFailure");
        THREAD_DUMP_ON_FAILURE = threadDumpOnFailure != null
                ? Boolean.parseBoolean(threadDumpOnFailure) : CiExecutionDetector.isOnCi();

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        boolean threadCPUTimeInfoAvailable = false;
        if (threadMXBean.isThreadCpuTimeSupported()) {
            try {
                threadMXBean.setThreadCpuTimeEnabled(true);
                threadCPUTimeInfoAvailable = true;
            } catch (Throwable ignored) {
            }
        }
        THREAD_CPU_TIME_INFO_AVAILABLE = threadCPUTimeInfoAvailable;

        boolean threadContentionInfoAvailable = false;
        if (threadMXBean.isThreadContentionMonitoringSupported()) {
            try {
                threadMXBean.setThreadContentionMonitoringEnabled(true);
                threadContentionInfoAvailable = true;
            } catch (Throwable ignored) {
            }
        }
        THREAD_CONTENTION_INFO_AVAILABLE = threadContentionInfoAvailable;
    }

    // initialize environment, logging and system properties
    private static void initialize() {
        // we set the JSR System properties globally, since some tests trigger the MBeanServer
        // initialization, which will not create the correct one without these System properties
        JsrTestUtil.setSystemProperties();

        if (isRunningCompatibilityTest()) {
            System.out.println("Running compatibility tests.");
            // Mock network cannot be used for compatibility testing
            System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, "true");
        } else {
            TestLoggingUtils.initializeLogging();
            if (System.getProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK) == null) {
                System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, "false");
            }
        }
        System.setProperty("hazelcast.phone.home.enabled", "false");
        System.setProperty("hazelcast.wait.seconds.before.join", "1");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");
        // override default async executor so it can report correct test names in test runs
        if (ForkJoinPool.getCommonPoolParallelism() > 1) {
            ConcurrencyUtil.setDefaultAsyncExecutor(new TestLoggingUtils.CustomTestNameAwareForkJoinPool());
        }
    }

    public static String getTestMethodName() {
        return TestNameHolder.getTestMethodName();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        String testName = context.getRequiredTestMethod().getName();
        TestNameHolder.setThreadLocalTestMethodName(testName);
        context.getStore(NAMESPACE).put(START_TIME_KEY, System.currentTimeMillis());
        System.out.println("Started Running Test: " + testName);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        try {
            long startTime = context.getStore(NAMESPACE).getOrDefault(START_TIME_KEY, Long.class, System.currentTimeMillis());
            float tookSeconds = (float) (System.currentTimeMillis() - startTime) / 1000;
            String testName = context.getRequiredTestMethod().getName();
            System.out.printf("Finished Running Test: %s in %.3f seconds.%n", testName, tookSeconds);

            if (context.getRequiredTestClass().isAnnotationPresent(QuickTest.class)) {
                logMessageIfTestOverran(testName, tookSeconds);
            }
        } finally {
            TestNameHolder.removeThreadLocalTestMethodName();
        }
    }

    @Override
    public void afterAll(@NonNull ExtensionContext context) throws Exception {
        // check for running Hazelcast instances
        Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
        if (!instances.isEmpty()) {
            String message = "Instances haven't been shut down: " + instances;
            HazelcastInstanceFactory.terminateAll();
            assertTrueEventually(HazelcastTestSupport::assertNoRunningInstances, 30);
            throw new IllegalStateException(message);
        }

        // check for running client instances
        Collection<HazelcastInstance> clientInstances = HazelcastClient.getAllHazelcastClients();
        if (!clientInstances.isEmpty()) {
            String message = "Client instances haven't been shut down: " + clientInstances;
            HazelcastClient.shutdownAll();
            assertTrueEventually(HazelcastTestSupport::assertNoRunningClientInstances, 30);
            throw new IllegalStateException(message);
        }

        // check for leftover JMX beans
        JmxLeakHelper.checkJmxBeans();

        // check for leftover CachingProvider instances
        int registrySize = getCachingProviderRegistrySize();
        if (registrySize > 0) {
            clearCachingProviderRegistry();
            throw new IllegalStateException(registrySize + " CachingProviders are not cleaned up."
                    + " Please use JsrTestUtil.cleanup() in your test!");
        }
    }

    @Override
    public void interceptTestMethod(@NonNull Invocation<Void> invocation,
                                    @NonNull ReflectiveInvocationContext<Method> invocationContext,
                                    @NonNull ExtensionContext extensionContext) throws Throwable {
        invoke(invocation, invocationContext, extensionContext);
    }

    @Override
    public void interceptTestTemplateMethod(@NonNull Invocation<Void> invocation,
                                            @NonNull ReflectiveInvocationContext<Method> invocationContext,
                                            @NonNull ExtensionContext extensionContext) throws Throwable {
        invoke(invocation, invocationContext, extensionContext);
    }

    @Override
    public void interceptAfterAllMethod(@NonNull Invocation<@Nullable Void> invocation,
                                        @NonNull ReflectiveInvocationContext<Method> invocationContext,
                                        @NonNull ExtensionContext extensionContext) throws Throwable {
        invoke(invocation, invocationContext, extensionContext);
    }

    @Override
    public void interceptAfterEachMethod(@NonNull Invocation<@Nullable Void> invocation,
                                         @NonNull ReflectiveInvocationContext<Method> invocationContext,
                                         @NonNull ExtensionContext extensionContext) throws Throwable {
        invoke(invocation, invocationContext, extensionContext);
    }

    @Override
    public void interceptBeforeEachMethod(@NonNull Invocation<@Nullable Void> invocation,
                                          @NonNull ReflectiveInvocationContext<Method> invocationContext,
                                          @NonNull ExtensionContext extensionContext) throws Throwable {
        invoke(invocation, invocationContext, extensionContext);
    }

    @Override
    public void interceptBeforeAllMethod(@NonNull Invocation<@Nullable Void> invocation,
                                         @NonNull ReflectiveInvocationContext<Method> invocationContext,
                                         @NonNull ExtensionContext extensionContext) throws Throwable {
        invoke(invocation, invocationContext, extensionContext);
    }

    private void invoke(Invocation<Void> invocation,
                        ReflectiveInvocationContext<Method> invocationContext,
                        ExtensionContext context) throws Throwable {
        Method method = invocationContext.getExecutable();

        try {
            invocation.proceed();
        } catch (Throwable e) {
            if (THREAD_DUMP_ON_FAILURE && !isJUnitAssumeException(e)) {
                System.err.println("THREAD DUMP FOR TEST FAILURE: \"" + method.getName() + "\"\n");
                try {
                    System.err.println(generateThreadDump());
                } catch (Throwable t) {
                    System.err.println("Unable to get thread dump!");
                }
            }
            throw e;
        }
    }
    private boolean isJUnitAssumeException(Throwable e) {
        return e instanceof AssumptionViolatedException || e instanceof TestAbortedException;
    }

    static void logMessageIfTestOverran(String methodName, float tookSeconds) {
        if (tookSeconds > QuickTest.EXPECTED_RUNTIME_THRESHOLD.getSeconds()) {
            System.err.println(MessageFormat.format(
                    "{0} is annotated as a {1}, expected to complete within {2} seconds - but took {3} seconds",
                    methodName, QuickTest.class.getSimpleName(),
                    QuickTest.EXPECTED_RUNTIME_THRESHOLD.getSeconds(), tookSeconds));
        }
    }

    private String generateThreadDump() {
        StringBuilder dump = new StringBuilder();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        long currentThreadId = Thread.currentThread().getId();
        for (ThreadInfo threadInfo : threadInfos) {
            long threadId = threadInfo.getThreadId();
            if (threadId == currentThreadId) {
                continue;
            }
            dump.append('"').append(threadInfo.getThreadName()).append("\" ");
            Thread.State state = threadInfo.getThreadState();
            dump.append("\n\tjava.lang.Thread.State: ").append(state);
            if (threadInfo.getLockName() != null) {
                dump.append(", on lock=").append(threadInfo.getLockName());
            }
            if (threadInfo.getLockOwnerName() != null) {
                dump.append(", owned by ").append(threadInfo.getLockOwnerName())
                        .append(", id=").append(threadInfo.getLockOwnerId());
            }
            if (THREAD_CPU_TIME_INFO_AVAILABLE) {
                dump.append(", cpu=").append(threadMXBean.getThreadCpuTime(threadId)).append(" nsecs");
                dump.append(", usr=").append(threadMXBean.getThreadUserTime(threadId)).append(" nsecs");
            }
            if (THREAD_CONTENTION_INFO_AVAILABLE) {
                dump.append(", blocked=").append(threadInfo.getBlockedTime()).append(" msecs");
                dump.append(", waited=").append(threadInfo.getWaitedTime()).append(" msecs");
            }
            for (StackTraceElement stackTraceElement : threadInfo.getStackTrace()) {
                dump.append("\n\t\tat ").append(stackTraceElement);
            }
            dump.append("\n\n");
        }
        return dump.toString();
    }
}
