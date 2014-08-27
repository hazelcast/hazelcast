package com.hazelcast.test;

import static java.lang.String.format;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class TestThread extends Thread {
    private volatile Throwable error;

    public TestThread() {
    }

    @Override
    public final void run() {
        try {
            doRun();
        } catch (Throwable t) {
            t.printStackTrace();
            this.error = t;
        }
    }

    public Throwable getError() {
        return error;
    }

    public abstract void doRun() throws Throwable;

    /**
     * Asserts that the thread eventually completes, no matter if there is an error or not.
     */
    public void assertTerminates() {
        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(format("Thread %s is still alive", getName()), isAlive());
            }
        });

    }

    /**
     * Assert that the thread eventually completes without an error.
     */
    public void assertSucceedsEventually() {
        assertTerminates();

        assertNull("No error should have been thrown, but " + getName() + " completed error", error);
    }

    /**
     * Asserts that the thread eventually completes with the expected error.
     *
     * @param cause
     */
    public void assertFailsEventually(Class<? extends Throwable> cause) {
        assertTerminates();
        assertNotNull("an error should have been thrown, but " + getName() + " completed without error", error);
        assertTrue("error instanceof " + error, error.getClass().isAssignableFrom(cause));
    }
}
