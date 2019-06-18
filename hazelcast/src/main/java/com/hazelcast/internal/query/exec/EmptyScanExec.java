package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.io.Row;

import java.util.function.Consumer;

/**
 * Scan over empty result-set.
 */
public class EmptyScanExec implements Exec {
    /** Singleton instance. */
    public static EmptyScanExec INSTANCE = new EmptyScanExec();

    private EmptyScanExec() {
        // No-op.
    }

    @Override
    public void setup(QueryContext ctx) {
        // No-op.
    }

    @Override
    public IterationResult next() {
        return IterationResult.FETCHED_DONE;
    }

    @Override
    public void consume(Consumer<Row> consumer) {
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public int remainingRows() {
        return 0;
    }
}
