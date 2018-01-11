/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractInvocationFuture_AbstractTest extends HazelcastTestSupport {

    protected ILogger logger;
    protected Executor executor;
    protected TestFuture future;
    protected Object value = "somevalue";

    @Before
    public void setup() {
        logger = Logger.getLogger(getClass());
        executor = Executors.newSingleThreadExecutor();
        future = new TestFuture();
    }


    class TestFuture extends AbstractInvocationFuture {
        volatile boolean interruptDetected;

        public TestFuture() {
            super(AbstractInvocationFuture_AbstractTest.this.executor, AbstractInvocationFuture_AbstractTest.this.logger);
        }

        public TestFuture(Executor executor, ILogger logger) {
            super(executor, logger);
        }

        @Override
        protected void onInterruptDetected() {
            interruptDetected = true;
            complete(new InterruptedException());
        }

        @Override
        protected String invocationToString() {
            return "someinvocation";
        }

        @Override
        protected Object resolveAndThrowIfException(Object state) throws ExecutionException, InterruptedException {
            if (state instanceof Throwable) {
                if (state instanceof Error) {
                    throw (Error) state;
                } else if (state instanceof RuntimeException) {
                    throw (RuntimeException) state;
                } else if (state instanceof InterruptedException) {
                    throw (InterruptedException) state;
                } else {
                    throw new ExecutionException((Throwable) state);
                }
            }
            return state;
        }

        @Override
        protected TimeoutException newTimeoutException(long timeout, TimeUnit unit) {
            return new TimeoutException();
        }
    }
}
