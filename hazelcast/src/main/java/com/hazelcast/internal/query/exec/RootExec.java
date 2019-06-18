package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.io.Row;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class RootExec extends AbstractExec {
    /** Actual executor. */
    private final Exec child;

    /** Mutex for proper future installation. */
    private final Object futureMux = new Object();

    /** Future notified when more results are available. */
    private CompletableFuture<Consumer<Row>> future;

    /** Whether executor is finished. */
    private boolean done;

    public RootExec(Exec child) {
        this.child = child;
    }

    @Override
    protected void setup0() {
        child.setup(ctx);
    }

    @Override
    public IterationResult next() {
        synchronized (futureMux) {
            if (done)
                return IterationResult.FETCHED_DONE;
        }

        IterationResult res = child.next();

        boolean done = false;
        RuntimeException err = null;

        switch (res) {
            case FETCHED:

                break;

            case FETCHED_DONE:
                done = true;

                break;

            case WAIT:
                // No results at the moment, wait.
                return IterationResult.WAIT;

            case ERROR:
                done = true;
        }

        synchronized (futureMux) {
            if (done) {
                this.done = true;

            }

            consumeAndNotifyFuture();
        }

        return null;
    }

    @Override
    public int remainingRows() {
        return child.remainingRows();
    }

    @Override
    public void consume(Consumer<Row> consumer) {
        child.consume(consumer);
    }

    /**
     * Install a future which will be notified when some results are available.
     *
     * @param future Future.
     */
    public void installFuture(CompletableFuture<Consumer<Row>> future) {
        synchronized (futureMux) {
            if (this.future != null)
                throw new IllegalStateException("Future is already installed.");

            this.future = future;

            // Notify immediately if result is ready.
            if (done)
                consumeAndNotifyFuture();
        }
    }

    private void consumeAndNotifyFuture() {
        assert Thread.holdsLock(futureMux);

        ArrayList<Row> rows = new ArrayList<>(child.remainingRows());

        child.consume(rows::add);

        // TODO: Remove print.
        System.out.println(">>> QUERY RESULT: ");

        for (Row row : rows)
            System.out.println("\t" + row);
    }
}
