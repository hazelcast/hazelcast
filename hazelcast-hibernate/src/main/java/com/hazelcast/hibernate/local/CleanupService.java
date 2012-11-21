package com.hazelcast.hibernate.local;

import com.hazelcast.impl.OutOfMemoryErrorDispatcher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @mdogan 11/14/12
 */
public final class CleanupService {

    private final ScheduledExecutorService executor;

    public CleanupService(final String name) {
        executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            public Thread newThread(final Runnable r) {
                return new Thread(r, name + ".hibernate.cleanup") {
                    public void run() {
                        try {
                            super.run();
                        } catch (OutOfMemoryError e) {
                            OutOfMemoryErrorDispatcher.onOutOfMemory(e);
                        }
                    }
                };
            }
        });
    }

    public void registerCache(final LocalRegionCache cache) {
        executor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                cache.cleanup();
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    public void stop() {
        executor.shutdownNow();
    }
}
