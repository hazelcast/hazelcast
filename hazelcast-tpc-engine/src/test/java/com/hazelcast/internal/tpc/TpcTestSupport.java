package com.hazelcast.internal.tpc;

import org.junit.Rule;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

    public static void sleepMillis(int millis) {
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
}
