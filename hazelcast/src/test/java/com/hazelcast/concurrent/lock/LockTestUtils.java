package com.hazelcast.concurrent.lock;

import com.hazelcast.core.ILock;

public class LockTestUtils {

    public static void lockByOtherThread(final ILock lock) {
        Thread t = new Thread() {
            public void run() {
                lock.lock();
            }
        };
        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
