package com.hazelcast.internal.query;

import com.hazelcast.internal.query.exec.RootExec;
import com.hazelcast.internal.query.io.Row;
import com.hazelcast.internal.query.io.RowBatch;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;

// TODO: Ability to close the Root from here (through cancellation routine)
public class QueryResultConsumerImpl implements QueryResultConsumer {
    /** Maximum size. */
    private final int maxSize;

    /** Mutex for concurrency support. */
    private final Object mux = new Object();

    /** Currently available rows. */
    private ArrayDeque<Row> rows = new ArrayDeque<>();

    /** Whether we are done. */
    private boolean done;

    /** Whether root update already scheduled. */
    private boolean rootScheduled;

    /** Query root. */
    private RootExec root;

    /** Iterator. */
    private InternalIterator iter;

    public QueryResultConsumerImpl(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public void setup(RootExec root) {
        synchronized (mux) {
            this.root = root;

            mux.notifyAll();
        }
    }

    @Override
    public int consume(RowBatch batch, int startPos) {
        synchronized (mux) {
            int available = batch.getRowCount() - startPos;
            int remaining = maxSize - rows.size();

            int toConsume = available > remaining ? remaining : available;

            for (int i = startPos; i < toConsume; i++)
                rows.add(batch.getRow(i));

            rootScheduled = false;

            mux.notifyAll();

            return toConsume;
        }
    }

    @Override
    public void done() {
        synchronized (mux) {
            done = true;

            mux.notifyAll();
        }
    }

    @Override
    public Iterator<Row> iterator() {
        if (iter != null)
            throw new IllegalStateException("Iterator can be opened only once.");

        iter = new InternalIterator();

        return iter;
    }

    /**
     * Iterator over results.
     */
    private class InternalIterator implements Iterator<Row> {
        /** Current row. */
        private Row currentRow;

        @Override
        public boolean hasNext() {
            advanceIfNeeded();

            return currentRow != null;
        }

        @Override
        public Row next() {
            advanceIfNeeded();

            Row res = currentRow;

            currentRow = null;

            if (res == null)
                throw new NoSuchElementException();

            return res;
        }

        private void advanceIfNeeded() {
            if (currentRow != null)
                return;

            currentRow = advance0();
        }

        private Row advance0() {
            synchronized (mux) {
                while (true) {
                    Row row = rows.poll();

                    if (row != null)
                        return row;

                    if (done)
                        return null;
                    else {
                        // Schedule root advance if needed.
                        if (root != null && !rootScheduled) {
                            root.reschedule();

                            rootScheduled = true;
                        }

                        try {
                            mux.wait();
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();

                            throw new RuntimeException("Thread was interrupted while waiting for more results.", e);
                        }
                    }
                }
            }
        }
    }
}
