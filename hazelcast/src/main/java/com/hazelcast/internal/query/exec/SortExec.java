package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryUtils;
import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.internal.query.io.EmptyRowBatch;
import com.hazelcast.internal.query.io.HeapRowBatch;
import com.hazelcast.internal.query.io.Row;
import com.hazelcast.internal.query.io.RowBatch;

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

    public SortExec(Exec upstream, List<Expression> expressions, List<Boolean> ascs) {
        this.upstream = upstream;
        this.expressions = expressions;

        map = new TreeMap<>(new SortKeyComparator(ascs));
    }

    @Override
    public IterationResult advance() {
        while (!upstreamDone) {
            switch (upstream.advance()) {
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

    private static class SortKey {

        private final List<Object> key;
        private final long idx;

        private SortKey(List<Object> key, long idx) {
            this.key = key;
            this.idx = idx;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(idx);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof SortKey) {
                SortKey other = (SortKey)obj;

                return other.idx == ((SortKey) obj).idx;
            }
            else
                return false;
        }
    }

    private static class SortKeyComparator implements Comparator<SortKey> {

        // TODO: Collation
        // TODO: NULLS FIRST/LAST
        // TODO: Type inference

        private final List<Boolean> ascs;

        private SortKeyComparator(List<Boolean> ascs) {
            this.ascs = ascs;
        }

        @Override
        public int compare(SortKey o1, SortKey o2) {
            int res;

            for (int i = 0; i < ascs.size(); i++) {
                boolean asc = ascs.get(i);

                Object item1 = o1.key.get(i);
                Object item2 = o2.key.get(i);

                res = QueryUtils.compare(item1, item2);

                if (asc)
                    res = -res;

                if (res != 0) {
                    if (!asc)
                        res = -res;

                    return res;
                }
            }

            return Long.compare(o1.idx, o2.idx);
        }
    }
}
