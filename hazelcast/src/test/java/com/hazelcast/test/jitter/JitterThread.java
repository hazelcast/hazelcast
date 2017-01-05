package com.hazelcast.test.jitter;


import static com.hazelcast.test.jitter.JitterRule.RESOLUTION_NANOS;
import static java.lang.Math.min;
import static java.util.concurrent.locks.LockSupport.parkNanos;

public class JitterThread extends Thread {

    private final JitterRecorder jitterRecorder;

    public JitterThread(JitterRecorder jitterRecorder) {
        this.jitterRecorder = jitterRecorder;
    }

    public void run() {
        long beforeNanos = System.nanoTime();
        long shortestHiccup = Long.MAX_VALUE;
        for (;;) {
            long beforeMillis = System.currentTimeMillis();
            sleepNanos(RESOLUTION_NANOS);
            long after = System.nanoTime();
            long delta = after - beforeNanos;
            long currentHiccup = delta - RESOLUTION_NANOS;

            //subtract the shortest observed hiccups. as that's
            //an inherit cost of the measuring loop and OS scheduler
            //imprecision.
            shortestHiccup = min(shortestHiccup, currentHiccup);
            currentHiccup -= shortestHiccup;

            jitterRecorder.recordPause(beforeMillis, currentHiccup);
            beforeNanos = after;
        }
    }

    private void sleepNanos(long duration) {
        parkNanos(duration);
    }

}
