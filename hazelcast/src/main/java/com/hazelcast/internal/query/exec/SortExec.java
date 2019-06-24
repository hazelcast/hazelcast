package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.QueryUtils;
import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.internal.query.io.EmptyRowBatch;
import com.hazelcast.internal.query.io.HeapRowBatch;
import com.hazelcast.internal.query.io.Row;
import com.hazelcast.internal.query.io.RowBatch;
import com.hazelcast.internal.query.sort.SortKey;
import com.hazelcast.internal.query.sort.SortKeyComparator;
import com.hazelcast.internal.query.worker.data.DataWorker;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

public class SortExec extends AbstractExec {

    private final Exec upstream;
    private final List<Expression> expressions;

    private final TreeMap<SortKey, Row> map;

    /** Resulting batch. */
    private RowBatch res;

    /** Whether upstream is finished. */
    private boolean upstreamDone;

    /** Index for unique elements. */
    private long idx;

    // TODO: Merge exprs and ascs to a single class, add null-first/last.
    public SortExec(Exec upstream, List<Expression> expressions, List<Boolean> ascs) {
        this.upstream = upstream;
        this.expressions = expressions;

        map = new TreeMap<>(new SortKeyComparator(ascs));
    }

    @Override
    protected void setup0(QueryContext ctx, DataWorker worker) {
        upstream.setup(ctx, worker);
    }

    @Override
    public IterationResult advance() {
        while (!upstreamDone) {
            IterationResult upstreamRes = upstream.advance();

            switch (upstreamRes) {
                case FETCHED_DONE:
                    upstreamDone = true;

                    // Fall-through.
                case FETCHED:
                    consumeBatch(upstream.currentBatch());

                    continue;

                case WAIT:
                    return IterationResult.WAIT;

                default:
                    // TODO: Implement error handling.
                    throw new UnsupportedOperationException("Implement me");
            }
        }

        // Blocking stage -> always end up in FETCHED_DONE state.
        finalizeResult();

        return IterationResult.FETCHED_DONE;
    }

    @Override
    public RowBatch currentBatch() {
        return res != null ? res : EmptyRowBatch.INSTANCE;
    }

    private void consumeBatch(RowBatch batch) {
        for (int i = 0; i < batch.getRowCount(); i++)
            consumeRow(batch.getRow(i));
    }

    private void consumeRow(Row row) {
        List<Object> key = new ArrayList<>(expressions.size());

        for (Expression expression : expressions)
            key.add(expression.eval(ctx, row));

        map.put(new SortKey(key, idx++), row);
    }

    private void finalizeResult() {
        if (res != null)
            return;

        List<Row> resList = new ArrayList<>(map.size());

        resList.addAll(map.values());

        res = new HeapRowBatch(resList);
    }
}
