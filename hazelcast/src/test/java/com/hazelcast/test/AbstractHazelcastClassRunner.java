/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.internal.runners.statements.RunAfters;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

/**
 * User: ahmetmircik
 * Date: 11/27/13
 */
public abstract class AbstractHazelcastClassRunner extends BlockJUnit4ClassRunner {

    protected static final boolean DISABLE_THREAD_DUMP_ON_FAILURE =
            Boolean.getBoolean("hazelcast.test.disableThreadDumpOnFailure");

    static {
        final String logging = "hazelcast.logging.type";
        if (System.getProperty(logging) == null) {
            System.setProperty(logging, "log4j");
        }
        if (System.getProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK) == null) {
            System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, "false");
        }
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.mancenter.enabled", "false");
        System.setProperty("hazelcast.wait.seconds.before.join", "1");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");

        // randomize multicast group...
        Random rand = new Random();
        int g1 = rand.nextInt(255);
        int g2 = rand.nextInt(255);
        int g3 = rand.nextInt(255);
        System.setProperty("hazelcast.multicast.group", "224." + g1 + "." + g2 + "." + g3);
    }

    /**
     * Creates a BlockJUnit4ClassRunner to run {@code klass}
     *
     * @throws org.junit.runners.model.InitializationError if the test class is malformed.
     */
    public AbstractHazelcastClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    protected List<FrameworkMethod> getChildren() {
        final List<FrameworkMethod> children = super.getChildren();
        Collections.shuffle(children);
        return children;
    }

    @Override
    protected Statement withAfters(FrameworkMethod method, Object target,
                                   Statement statement) {
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

    protected class ThreadDumpAwareRunAfters extends Statement {

        private final FrameworkMethod method;
        private final Statement next;
        private final Object target;
        private final List<FrameworkMethod> afters;

        protected ThreadDumpAwareRunAfters(FrameworkMethod method, Statement next,
                                           List<FrameworkMethod> afters, Object target) {
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
                System.err.println("THREAD DUMP FOR TEST FAILURE: " +
                                   "\"" + e.getMessage() + "\" at " +
                                   "\"" + method.getName() + "\"" + "\n");
                System.err.println(generateThreadDump());
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

    @Override
    protected Statement methodBlock(FrameworkMethod method) {
        final Statement statement = super.methodBlock(method);
        final Repeat repeatable = getRepeatable(method);
        if (repeatable == null || repeatable.value() < 2) {
            return statement;
        }
        return new TestRepeater(statement, method.getMethod(), repeatable.value());
    }

    /**
     * Gets repeat annotation, if any.
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
    protected Statement withAfterClasses(Statement statement) {
        final Statement originalStatement = super.withAfterClasses(statement);

        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                originalStatement.evaluate();

                Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
                if(!instances.isEmpty()) {
                    String message = "Instances haven't been shut down: " + instances;
                    Hazelcast.shutdownAll();
                    throw new IllegalStateException(message);
                }
            }
        };
    }

    protected class TestRepeater extends Statement {

        private final Statement statement;
        private final Method testMethod;
        private final int repeat;

        protected TestRepeater(Statement statement, Method testMethod, int repeat) {
            this.statement = statement;
            this.testMethod = testMethod;
            this.repeat = Math.max(1, repeat);
        }

        /**
         * Invokes the next {@link Statement statement} in the execution chain for
         * the specified repeat count.
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

    protected String generateThreadDump() {
        final StringBuilder dump = new StringBuilder();
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        final long currentThreadId = Thread.currentThread().getId();
        for (ThreadInfo threadInfo : threadInfos) {
            if (threadInfo.getThreadId() == currentThreadId) {
                continue;
            }
            dump.append('"');
            dump.append(threadInfo.getThreadName());
            dump.append("\" ");
            final Thread.State state = threadInfo.getThreadState();
            dump.append("\n\tjava.lang.Thread.State: ");
            dump.append(state);
            if (threadInfo.getLockName() != null) {
                dump.append(" on lock=" + threadInfo.getLockName());
            }
            if (threadInfo.getLockOwnerName() != null) {
                dump.append(" owned by " + threadInfo.getLockOwnerName() + " id=" + threadInfo.getLockOwnerId());
            }
            final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
            for (StackTraceElement stackTraceElement : stackTraceElements) {
                dump.append("\n\t\tat ");
                dump.append(stackTraceElement);
            }
            dump.append("\n\n");
        }
        return dump.toString();
    }

}
