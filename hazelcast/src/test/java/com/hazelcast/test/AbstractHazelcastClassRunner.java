/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.annotation.Repeat;
import org.apache.log4j.MDC;
import org.junit.After;
import org.junit.Test;
import org.junit.internal.runners.statements.RunAfters;
import org.junit.rules.TestRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;

import java.lang.annotation.Annotation;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.Boolean.getBoolean;
import static java.lang.Integer.getInteger;

/**
 * Base test runner which has base system properties and test repetition logic. The tests are run in random order.
 */
public abstract class AbstractHazelcastClassRunner extends AbstractParameterizedHazelcastClassRunner {

    protected static final boolean DISABLE_THREAD_DUMP_ON_FAILURE = getBoolean("hazelcast.test.disableThreadDumpOnFailure");
    protected static final int DEFAULT_TEST_TIMEOUT_IN_SECONDS = getInteger("hazelcast.test.defaultTestTimeoutInSeconds", 300);

    private static final ThreadLocal<String> TEST_NAME_THREAD_LOCAL = new InheritableThreadLocal<String>();
    private static final boolean THREAD_CPU_TIME_INFO_AVAILABLE;
    private static final boolean THREAD_CONTENTION_INFO_AVAILABLE;

    static {
        String logging = "hazelcast.logging.type";
        if (System.getProperty(logging) == null) {
            System.setProperty(logging, "log4j");
        }
        if (System.getProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK) == null) {
            System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, "false");
        }
        System.setProperty("hazelcast.phone.home.enabled", "false");
        System.setProperty("hazelcast.mancenter.enabled", "false");
        System.setProperty("hazelcast.wait.seconds.before.join", "1");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");

        // randomize multicast group
        Random rand = new Random();
        int g1 = rand.nextInt(255);
        int g2 = rand.nextInt(255);
        int g3 = rand.nextInt(255);
        System.setProperty("hazelcast.multicast.group", "224." + g1 + "." + g2 + "." + g3);

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        boolean threadCPUTimeInfoAvailable = false;
        if (threadMXBean.isThreadCpuTimeSupported()) {
            try {
                threadMXBean.setThreadCpuTimeEnabled(true);
                threadCPUTimeInfoAvailable = true;
            } catch (Throwable t) {
            }
        }
        THREAD_CPU_TIME_INFO_AVAILABLE = threadCPUTimeInfoAvailable;

        boolean threadContentionInfoAvailable = false;
        if (threadMXBean.isThreadContentionMonitoringSupported()) {
            try {
                threadMXBean.setThreadContentionMonitoringEnabled(true);
                threadContentionInfoAvailable = true;
            } catch (Throwable t) {
            }
        }
        THREAD_CONTENTION_INFO_AVAILABLE = threadContentionInfoAvailable;
    }

    /**
     * Creates a BlockJUnit4ClassRunner to run {@code clazz}
     *
     * @throws org.junit.runners.model.InitializationError if the test class is malformed.
     */
    public AbstractHazelcastClassRunner(Class<?> clazz) throws InitializationError {
        super(clazz);
    }

    public AbstractHazelcastClassRunner(Class<?> clazz, Object[] parameters, String name) throws InitializationError {
        super(clazz, parameters, name);
    }

    protected static void setThreadLocalTestMethodName(String name) {
        MDC.put("test-name", name);
        TEST_NAME_THREAD_LOCAL.set(name);
    }

    protected static void removeThreadLocalTestMethodName() {
        TEST_NAME_THREAD_LOCAL.remove();
        MDC.remove("test-name");
    }

    public static String getTestMethodName() {
        return TEST_NAME_THREAD_LOCAL.get();
    }

    @Override
    protected List<FrameworkMethod> getChildren() {
        List<FrameworkMethod> children = super.getChildren();
        List<FrameworkMethod> modifiableList = new ArrayList<FrameworkMethod>(children);
        Collections.shuffle(modifiableList);
        return modifiableList;
    }

    @Override
    protected Statement withPotentialTimeout(FrameworkMethod method, Object test, Statement next) {
        long timeout = getTimeout(method.getAnnotation(Test.class));
        return new FailOnTimeoutStatement(method.getName(), next, timeout);
    }

    private long getTimeout(Test annotation) {
        if (annotation == null || annotation.timeout() == 0) {
            return TimeUnit.SECONDS.toMillis(DEFAULT_TEST_TIMEOUT_IN_SECONDS);
        }
        return annotation.timeout();
    }

    @Override
    protected Statement withAfters(FrameworkMethod method, Object target, Statement statement) {
        List<FrameworkMethod> afters = getTestClass().getAnnotatedMethods(After.class);
        if (!DISABLE_THREAD_DUMP_ON_FAILURE) {
            return new ThreadDumpAwareRunAfters(method, statement, afters, target);
        }
        if (afters.isEmpty()) {
            return statement;
        } else {
            return new RunAfters(statement, afters, target);
        }
    }

    @Override
    protected Statement methodBlock(FrameworkMethod method) {
        Statement statement = super.methodBlock(method);
        Repeat repeatable = getRepeatable(method);
        if (repeatable == null || repeatable.value() < 2) {
            return statement;
        }
        return new TestRepeater(statement, method.getMethod(), repeatable.value());
    }

    /**
     * Gets the {@link Repeat} annotation if set.
     * <p/>
     * Method level definition overrides class level definition.
     */
    private Repeat getRepeatable(FrameworkMethod method) {
        Repeat repeatable = method.getAnnotation(Repeat.class);
        if (repeatable == null) {
            repeatable = super.getTestClass().getJavaClass().getAnnotation(Repeat.class);
        }
        return repeatable;
    }

    @Override
    protected List<TestRule> getTestRules(Object target) {
        List<TestRule> testRules = super.getTestRules(target);

        Set<Class<? extends TestRule>> testRuleClasses = new HashSet<Class<? extends TestRule>>();

        TestClass testClass = getTestClass();

        // Find the required test rule classes from test class itself
        Annotation[] classAnnotations = testClass.getAnnotations();
        for (Annotation annotation : classAnnotations) {
            Class<? extends Annotation> annotationType = annotation.annotationType();
            AutoRegisteredTestRule autoFilterRule = annotationType.getAnnotation(AutoRegisteredTestRule.class);
            if (autoFilterRule != null) {
                Class<? extends TestRule> testRuleClass = autoFilterRule.testRule();
                testRuleClasses.add(testRuleClass);
            }
        }

        // Find the required test rule classes from methods
        List<FrameworkMethod> annotatedMethods = testClass.getAnnotatedMethods();
        for (FrameworkMethod annotatedMethod : annotatedMethods) {
            Annotation[] methodAnnotations = annotatedMethod.getAnnotations();
            for (Annotation annotation : methodAnnotations) {
                Class<? extends Annotation> annotationType = annotation.annotationType();
                AutoRegisteredTestRule autoFilterRule = annotationType.getAnnotation(AutoRegisteredTestRule.class);
                if (autoFilterRule != null) {
                    Class<? extends TestRule> testRuleClass = autoFilterRule.testRule();
                    testRuleClasses.add(testRuleClass);
                }
            }
        }

        // Create and register test rules
        for (Class<? extends TestRule> testRuleClass : testRuleClasses) {
            try {
                TestRule testRule = testRuleClass.newInstance();
                testRules.add(testRule);
            } catch (Throwable t) {
                System.err.println("Unable to create test rule instance of test rule class "
                        + testRuleClass.getName() + " because of " + t);
            }
        }

        return testRules;
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
            }
        };
    }

    protected String generateThreadDump() {
        StringBuilder dump = new StringBuilder();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        long currentThreadId = Thread.currentThread().getId();
        for (ThreadInfo threadInfo : threadInfos) {
            long threadId = threadInfo.getThreadId();
            if (threadId == currentThreadId) {
                continue;
            }
            dump.append('"');
            dump.append(threadInfo.getThreadName());
            dump.append("\" ");

            Thread.State state = threadInfo.getThreadState();
            dump.append("\n\tjava.lang.Thread.State: ");
            dump.append(state);
            if (threadInfo.getLockName() != null) {
                dump.append(", on lock=").append(threadInfo.getLockName());
            }
            if (threadInfo.getLockOwnerName() != null) {
                dump.append(", owned by ").append(threadInfo.getLockOwnerName());
                dump.append(", id=").append(threadInfo.getLockOwnerId());
            }
            if (THREAD_CPU_TIME_INFO_AVAILABLE) {
                dump.append(", cpu=").append(threadMXBean.getThreadCpuTime(threadId)).append(" nsecs");
                dump.append(", usr=").append(threadMXBean.getThreadUserTime(threadId)).append(" nsecs");
            }
            if (THREAD_CONTENTION_INFO_AVAILABLE) {
                dump.append(", blocked=").append(threadInfo.getBlockedTime()).append(" msecs");
                dump.append(", waited=").append(threadInfo.getWaitedTime()).append(" msecs");
            }
            StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
            for (StackTraceElement stackTraceElement : stackTraceElements) {
                dump.append("\n\t\tat ");
                dump.append(stackTraceElement);
            }
            dump.append("\n\n");
        }
        return dump.toString();
    }

    protected class ThreadDumpAwareRunAfters extends Statement {

        private final FrameworkMethod method;
        private final Statement next;
        private final Object target;
        private final List<FrameworkMethod> afters;

        protected ThreadDumpAwareRunAfters(FrameworkMethod method, Statement next, List<FrameworkMethod> afters, Object target) {
            this.method = method;
            this.next = next;
            this.afters = afters;
            this.target = target;
        }

        @Override
        public void evaluate() throws Throwable {
            List<Throwable> errors = new ArrayList<Throwable>();
            try {
                next.evaluate();
            } catch (Throwable e) {
                System.err.println("THREAD DUMP FOR TEST FAILURE: \"" + e.getMessage() + "\" at \"" + method.getName() + "\"\n");
                try {
                    System.err.println(generateThreadDump());
                } catch (Throwable t) {
                    System.err.println("Unable to get thread dump!");
                    e.printStackTrace();
                }
                errors.add(e);
            } finally {
                for (FrameworkMethod each : afters) {
                    try {
                        each.invokeExplosively(target);
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                }
            }
            MultipleFailureException.assertEmpty(errors);
        }
    }

    private class TestRepeater extends Statement {

        private final Statement statement;
        private final Method testMethod;
        private final int repeat;

        protected TestRepeater(Statement statement, Method testMethod, int repeat) {
            this.statement = statement;
            this.testMethod = testMethod;
            this.repeat = Math.max(1, repeat);
        }

        /**
         * Invokes the next {@link Statement statement} in the execution chain for the specified repeat count.
         */
        @Override
        public void evaluate() throws Throwable {
            for (int i = 0; i < repeat; i++) {
                if (repeat > 1) {
                    System.out.println(String.format("---> Repeating test [%s:%s], run count [%d]",
                            testMethod.getDeclaringClass().getCanonicalName(),
                            testMethod.getName(), i + 1));
                }
                statement.evaluate();
            }
        }
    }
}
