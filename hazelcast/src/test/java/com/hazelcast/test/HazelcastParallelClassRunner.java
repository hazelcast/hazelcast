/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.annotation.TestProperties;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.max;
import static java.lang.Runtime.getRuntime;


/**
 * Runs the tests in parallel with multiple threads.
 */
public class HazelcastParallelClassRunner extends AbstractHazelcastClassRunner {
    private static final boolean SPAWN_MULTIPLE_THREADS = TestEnvironment.isMockNetwork() && !Boolean.getBoolean("multipleJVM");
    private static final int MAX_THREADS = max(getRuntime().availableProcessors()/2, 1);

    private final AtomicInteger numThreads = new AtomicInteger(0);
    private final int maxThreads;

    public HazelcastParallelClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
        maxThreads = getMaxThreads(klass);
    }

    public HazelcastParallelClassRunner(Class<?> klass, Object[] parameters,
                                        String name) throws InitializationError {
        super(klass, parameters, name);
        maxThreads =  getMaxThreads(klass);
    }

    private int getMaxThreads(Class<?> klass) {
        if (!SPAWN_MULTIPLE_THREADS) {
            return 1;
        }
        
        TestProperties properties = klass.getAnnotation(TestProperties.class);

        if (properties != null) {
            Class<? extends MaxThreadsAware> clazz = properties.maxThreadsCalculatorClass();

            try {
                Constructor c = clazz.getConstructor();
                MaxThreadsAware maxThreadsAware = (MaxThreadsAware) c.newInstance();
                return maxThreadsAware.maxThreads();
            } catch (Throwable e) {
                return MAX_THREADS;
            }
        } else {
            return MAX_THREADS;
        }
    }

    @Override
    protected void runChild(final FrameworkMethod method, final RunNotifier notifier) {
        while (numThreads.get() >= maxThreads) {
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        numThreads.incrementAndGet();
        new Thread(new TestRunner(method, notifier), method.getName()).start();
    }

    @Override
    protected Statement childrenInvoker(final RunNotifier notifier) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                HazelcastParallelClassRunner.super.childrenInvoker(notifier).evaluate();
                // wait for all child threads (tests) to complete
                while (numThreads.get() > 0) {
                    Thread.sleep(25);
                }
            }
        };
    }

    private class TestRunner implements Runnable {
        private final FrameworkMethod method;
        private final RunNotifier notifier;

        public TestRunner(final FrameworkMethod method, final RunNotifier notifier) {
            this.method = method;
            this.notifier = notifier;
        }

        @Override
        public void run() {
            FRAMEWORK_METHOD_THREAD_LOCAL.set(method);
            try {
                long start = System.currentTimeMillis();
                String testName = method.getMethod().getDeclaringClass().getSimpleName() + "." + method.getName();
                System.out.println("Started Running Test: " + testName);
                HazelcastParallelClassRunner.super.runChild(method, notifier);
                numThreads.decrementAndGet();
                float took = (float) (System.currentTimeMillis() - start) / 1000;
                System.out.println(String.format("Finished Running Test: %s in %.3f seconds.", testName, took));
            } finally {
                FRAMEWORK_METHOD_THREAD_LOCAL.remove();
            }
        }
    }
}
