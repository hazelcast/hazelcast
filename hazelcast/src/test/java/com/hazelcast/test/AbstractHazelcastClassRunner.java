/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.starter.ReflectionUtils;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.runners.statements.RunAfters;
import org.junit.internal.runners.statements.RunBefores;
import org.junit.rules.ExpectedException;
import org.junit.rules.ExpectedExceptionMatcherBuilderAccessor;
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
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.test.TestEnvironment.isRunningCompatibilityTest;
import static java.lang.Integer.getInteger;

/**
 * Base test runner which has base system properties and test repetition logic.
 * <p>
 * The tests are executed in random order.
 */
public abstract class AbstractHazelcastClassRunner extends AbstractParameterizedHazelcastClassRunner {

    private static final int DEFAULT_TEST_TIMEOUT_IN_SECONDS = getInteger("hazelcast.test.defaultTestTimeoutInSeconds", 300);
    private static final boolean THREAD_DUMP_ON_FAILURE;

    private static final ThreadLocal<String> TEST_NAME_THREAD_LOCAL = new InheritableThreadLocal<String>();
    private static final boolean THREAD_CPU_TIME_INFO_AVAILABLE;
    private static final boolean THREAD_CONTENTION_INFO_AVAILABLE;

    static {
        initialize();

        final String threadDumpOnFailure = System.getProperty("hazelcast.test.threadDumpOnFailure");
        THREAD_DUMP_ON_FAILURE = threadDumpOnFailure != null
                ? Boolean.parseBoolean(threadDumpOnFailure) : JenkinsDetector.isOnJenkins();

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        boolean threadCPUTimeInfoAvailable = false;
        if (threadMXBean.isThreadCpuTimeSupported()) {
            try {
                threadMXBean.setThreadCpuTimeEnabled(true);
                threadCPUTimeInfoAvailable = true;
            } catch (Throwable t) {
                ignore(t);
            }
        }
        THREAD_CPU_TIME_INFO_AVAILABLE = threadCPUTimeInfoAvailable;

        boolean threadContentionInfoAvailable = false;
        if (threadMXBean.isThreadContentionMonitoringSupported()) {
            try {
                threadMXBean.setThreadContentionMonitoringEnabled(true);
                threadContentionInfoAvailable = true;
            } catch (Throwable t) {
                ignore(t);
            }
        }
        THREAD_CONTENTION_INFO_AVAILABLE = threadContentionInfoAvailable;
    }

    // initialize environment, logging and attach a final-modifier removing agent if required
    private static void initialize() {
        // we set the JSR System properties globally, since some tests trigger the MBeanServer
        // initialization, which will not create the correct one without these System properties
        // (this was done via the pom.xml before for most test profiles, so this does no harm)
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
        //override default async executor of hazelcast so that it can report correct test names in test runs
        //if ForkJoinPool parallelism is less than or equal to 1, `thread-per-task` will be used.
        //In that case there is no need to override defaultAsyncExecutor
        if (ForkJoinPool.getCommonPoolParallelism() > 1) {
            ConcurrencyUtil.setDefaultAsyncExecutor(new TestLoggingUtils.CustomTestNameAwareForkJoinPool());
        }
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

    public static String getTestMethodName() {
        return TEST_NAME_THREAD_LOCAL.get();
    }

    static void setThreadLocalTestMethodName(String name) {
        TestLoggingUtils.setThreadLocalTestMethodName(name);
        TEST_NAME_THREAD_LOCAL.set(name);
    }

    static void removeThreadLocalTestMethodName() {
        TestLoggingUtils.removeThreadLocalTestMethodName();
        TEST_NAME_THREAD_LOCAL.remove();
    }

    @Override
    protected List<FrameworkMethod> getChildren() {
        List<FrameworkMethod> children = super.getChildren();
        List<FrameworkMethod> modifiableList = new ArrayList<FrameworkMethod>(children);
        Collections.shuffle(modifiableList);
        return modifiableList;
    }

    @Override
    @SuppressWarnings("deprecation")
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
        Statement nextStatement = statement;
        List<TestRule> testRules = getTestRules(target);
        ExpectedException expectedException = null;
        if (!testRules.isEmpty()) {
            for (TestRule rule : testRules) {
                if (rule instanceof BounceMemberRule) {
                    nextStatement = ((BounceMemberRule) rule).stopBouncing(statement);
                }
                if (rule instanceof ExpectedException) {
                    expectedException = (ExpectedException) rule;
                }
            }
        }
        if (THREAD_DUMP_ON_FAILURE) {
            return new ThreadDumpAwareRunAfters(method, nextStatement, afters, target, expectedException);
        }
        if (afters.isEmpty()) {
            return nextStatement;
        } else {
            return new RunAfters(nextStatement, afters, target);
        }
    }

    // Override withBefores to accommodate spawning the member bouncing thread after @Before's have been executed and before @Test
    // when MemberBounceRule is in use
    @Override
    protected Statement withBefores(FrameworkMethod method, Object target,
                                    Statement statement) {
        List<FrameworkMethod> befores = getTestClass().getAnnotatedMethods(
                Before.class);
        List<TestRule> testRules = getTestRules(target);
        Statement nextStatement = statement;
        if (!testRules.isEmpty()) {
            for (TestRule rule : testRules) {
                if (rule instanceof BounceMemberRule) {
                    nextStatement = ((BounceMemberRule) rule).startBouncing(statement);
                }
            }
        }
        return befores.isEmpty() ? nextStatement : new RunBefores(nextStatement,
                befores, target);
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
     * <p>
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

        // find the required test rule classes from test class itself
        Annotation[] classAnnotations = testClass.getAnnotations();
        for (Annotation annotation : classAnnotations) {
            Class<? extends Annotation> annotationType = annotation.annotationType();
            AutoRegisteredTestRule autoFilterRule = annotationType.getAnnotation(AutoRegisteredTestRule.class);
            if (autoFilterRule != null) {
                Class<? extends TestRule> testRuleClass = autoFilterRule.testRule();
                testRuleClasses.add(testRuleClass);
            }
        }

        // find the required test rule classes from methods
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

        // create and register test rules
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
        return new AfterClassesStatement(originalStatement);
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
        private final ExpectedException expectedException;

        ThreadDumpAwareRunAfters(FrameworkMethod method, Statement next, List<FrameworkMethod> afters, Object target,
                                 ExpectedException expectedException) {
            this.method = method;
            this.next = next;
            this.afters = afters;
            this.target = target;
            this.expectedException = expectedException;
        }

        @Override
        public void evaluate() throws Throwable {
            List<Throwable> errors = new ArrayList<Throwable>();
            try {
                next.evaluate();
            } catch (Throwable e) {
                if (!isJUnitAssumeException(e) && !expectsException()) {
                    System.err.println("THREAD DUMP FOR TEST FAILURE: \"" + e.getMessage()
                            + "\" at \"" + method.getName() + "\"\n");
                    try {
                        System.err.println(generateThreadDump());
                    } catch (Throwable t) {
                        System.err.println("Unable to get thread dump!");
                        e.printStackTrace();
                    }
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

        private boolean isJUnitAssumeException(Throwable e) {
            return e instanceof AssumptionViolatedException;
        }

        private boolean expectsException() throws IllegalAccessException {
            if (expectedException == null) {
                return false;
            }
            Object exceptionMatcher = ReflectionUtils.getFieldValueReflectively(expectedException, "matcherBuilder");
            return ExpectedExceptionMatcherBuilderAccessor.expectsThrowable(exceptionMatcher);
        }
    }

    private class TestRepeater extends Statement {

        private final Statement statement;
        private final Method testMethod;
        private final int repeat;

        TestRepeater(Statement statement, Method testMethod, int repeat) {
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
