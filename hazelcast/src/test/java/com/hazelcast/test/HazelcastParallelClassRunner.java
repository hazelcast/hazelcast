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

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.util.concurrent.atomic.AtomicInteger;

public class HazelcastParallelClassRunner extends AbstractHazelcastClassRunner {

    private static final int MAX_THREADS;

    static {
        int cores = Runtime.getRuntime().availableProcessors();
        if (!TestEnvironment.isMockNetwork()) {
            MAX_THREADS = 1;
        } else if (cores < 8) {
            MAX_THREADS = 8;
        } else {
            MAX_THREADS = cores;
        }
    }

    private final AtomicInteger numThreads;

    public HazelcastParallelClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
        numThreads = new AtomicInteger(0);
    }

    @Override
    protected void runChild(final FrameworkMethod method, final RunNotifier notifier) {
        while (numThreads.get() >= MAX_THREADS) {
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                System.err.println("Interrupted: " + method.getName());
                e.printStackTrace();
                return; // The user may have interrupted us; this won't happen normally
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
            long start = System.currentTimeMillis();
            String testName = method.getMethod().getDeclaringClass().getSimpleName() + "." + method.getName();
            System.out.println("Started Running Test: " + testName);
            HazelcastParallelClassRunner.super.runChild(method, notifier);
            numThreads.decrementAndGet();
            float took = (float) (System.currentTimeMillis() - start) / 1000;
            System.out.println(String.format("Finished Running Test: %s in %.3f seconds.", testName, took));
        }
    }

}
