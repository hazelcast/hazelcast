package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.internal.query.row.EmptyRowBatch;
import com.hazelcast.internal.query.row.ListRowBatch;
import com.hazelcast.internal.query.row.Row;
import com.hazelcast.internal.query.row.RowBatch;
import com.hazelcast.internal.query.sort.SortKey;
import com.hazelcast.internal.query.sort.SortKeyComparator;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class SortExec extends AbstractUpstreamAwareExec {

    private final List<Expression> expressions;

    private final TreeMap<SortKey, Row> map;

    /** Resulting batch. */
    private RowBatch res;

    /** Index for unique elements. */
    private long idx;

    // TODO: Merge exprs and ascs to a single class, add null-first/last.
    public SortExec(Exec upstream, List<Expression> expressions, List<Boolean> ascs) {
        super(upstream);

        this.expressions = expressions;

        map = new TreeMap<>(new SortKeyComparator(ascs));
    }

    @Override
    public IterationResult advance() {
        while (!upstreamDone) {
            switch (advanceUpstream()) {
                case FETCHED_DONE:
                case FETCHED:
                    consumeBatch(upstreamCurrentBatch);

                    continue;

                case WAIT:
                    return IterationResult.WAIT;

                default:
                    throw new IllegalStateException("Should not reach this.");
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

        res = new ListRowBatch(resList);
    }
}
