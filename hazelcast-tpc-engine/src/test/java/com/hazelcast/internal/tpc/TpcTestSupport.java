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

package com.hazelcast.internal.tpc;

import com.hazelcast.internal.tpc.iobuffer.IOBuffer;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Integer.getInteger;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TpcTestSupport {

    public static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT = getInteger("hazelcast.assertTrueEventually.timeout", 120);
    public static final int TERMINATION_TIMEOUT_SECONDS = 30;

    public static void assertCompletesEventually(final Future future) {
        assertTrueEventually(() -> assertTrue("Future has not completed", future.isDone()));
    }

    public static void terminateAll(Collection<? extends Reactor> reactors) {
        if (reactors == null) {
            return;
        }

        for (Reactor reactor : reactors) {
            if (reactor == null) {
                continue;
            }
            reactor.shutdown();
        }

        for (Reactor reactor : reactors) {
            if (reactor == null) {
                continue;
            }
            try {
                if (!reactor.awaitTermination(TERMINATION_TIMEOUT_SECONDS, SECONDS)) {
                    throw new RuntimeException("Reactor failed to terminate within timeout.");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void terminate(Reactor reactor) {
        if (reactor == null) {
            return;
        }

        reactor.shutdown();
        try {
            if (!reactor.awaitTermination(TERMINATION_TIMEOUT_SECONDS, SECONDS)) {
                throw new RuntimeException("Reactor failed to terminate within timeout.");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void constructComplete(IOBuffer buff) {
        buff.putInt(0, buff.position());
        buff.byteBuffer().flip();
    }

    public static void assertTrueFiveSeconds(AssertTask task) {
        assertTrueAllTheTime(task, 5);
    }

    public static void assertTrueTwoSeconds(AssertTask task) {
        assertTrueAllTheTime(task, 2);
    }

    public static void assertTrueAllTheTime(AssertTask task, long durationSeconds) {
        for (int i = 0; i <= durationSeconds; i++) {
            try {
                task.run();
            } catch (Exception e) {
                throw rethrow(e);
            }
            // Don't wait if there is not next iteration
            if ((i + 1) <= durationSeconds) {
                sleepMillis(SECONDS.toMillis(1));
            }
        }
    }

    public static void sleepMillis(long millis) {
        try {
            MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static <E> E assertInstanceOf(Class<E> expected, Object actual) {
        assertNotNull(actual);
        assertTrue(actual + " is not an instanceof " + expected.getName(), expected.isAssignableFrom(actual.getClass()));
        return (E) actual;
    }

    public static void assertOpenEventually(CountDownLatch latch) {
        assertOpenEventually(latch, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertOpenEventually(CountDownLatch latch, long timeoutSeconds) {
        assertTrueEventually(() -> {
            boolean success = latch.await(timeoutSeconds, SECONDS);
            assertTrue(success);
        }, timeoutSeconds);
    }

    public static <E> void assertEqualsEventually(final Callable<E> task, final E expected) {
        assertTrueEventually(() -> assertEquals(expected, task.call()));
    }

    public static void assertEqualsEventually(final int expected, final AtomicInteger value) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(expected, value.get());
            }
        });
    }

    public static void assertTrueEventually(AssertTask task) {
        assertTrueEventually(null, task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertTrueEventually(String message, AssertTask task, long timeoutSeconds) {
        AssertionError error = null;
        // we are going to check five times a second
        int sleepMillis = 200;
        long iterations = timeoutSeconds * 5;
        long deadline = System.currentTimeMillis() + SECONDS.toMillis(timeoutSeconds);
        for (int i = 0; i < iterations && System.currentTimeMillis() < deadline; i++) {
            try {
                try {
                    task.run();
                } catch (Exception e) {
                    throw rethrow(e);
                }
                return;
            } catch (AssertionError e) {
                error = e;
            }
            sleepMillis(sleepMillis);
        }
        if (error != null) {
            throw error;
        }
        fail("assertTrueEventually() failed without AssertionError! " + message);
    }

    public static RuntimeException rethrow(Throwable t) {
        if (t instanceof Error) {
            throw (Error) t;
        }

        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }

        throw new RuntimeException(t);
    }

    public static void assertTrueEventually(AssertTask task, long timeoutSeconds) {
        assertTrueEventually(null, task, timeoutSeconds);
    }

    /**
     * Note: the {@code cancel()} method on the returned future has no effect.
     */
    public static Future spawn(Runnable task) {
        FutureTask<Runnable> futureTask = new FutureTask<>(task, null);
        new Thread(futureTask).start();
        return futureTask;
    }

    /**
     * Note: the {@code cancel()} method on the returned future has no effect.
     */
    public static <E> Future<E> spawn(Callable<E> task) {
        FutureTask<E> futureTask = new FutureTask<E>(task);
        new Thread(futureTask).start();
        return futureTask;
    }
}
