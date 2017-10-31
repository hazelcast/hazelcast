package com.hazelcast.raft.impl.util;

import com.hazelcast.util.executor.StripedRunnable;

import java.util.concurrent.Executor;

/**
 * TODO: Javadoc Pending...
 *
 */
public class StripedExecutorConveyor implements Executor {
    private final int key;
    private final Executor executor;

    public StripedExecutorConveyor(int key, Executor executor) {
        this.key = key;
        this.executor = executor;
    }

    @Override
    public void execute(final Runnable command) {
        executor.execute(new StripedRunnable() {
            @Override
            public int getKey() {
                return key;
            }

            @Override
            public void run() {
                command.run();
            }
        });
    }
}
