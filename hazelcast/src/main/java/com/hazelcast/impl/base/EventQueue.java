/**
 * 
 */
package com.hazelcast.impl.base;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class EventQueue extends ConcurrentLinkedQueue<Runnable> implements Runnable {
    private AtomicInteger size = new AtomicInteger();

    public int offerRunnable(Runnable runnable) {
        offer(runnable);
        return size.incrementAndGet();
    }

    public void run() {
        while (true) {
            final Runnable eventTask = poll();
            if (eventTask != null) {
                eventTask.run();
                size.decrementAndGet();
            } else {
                return;
            }
        }
    }
}