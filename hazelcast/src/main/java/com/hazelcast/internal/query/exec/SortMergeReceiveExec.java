package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.internal.query.row.ListRowBatch;
import com.hazelcast.internal.query.row.Row;
import com.hazelcast.internal.query.row.RowBatch;
import com.hazelcast.internal.query.mailbox.SendBatch;
import com.hazelcast.internal.query.mailbox.StripedInbox;
import com.hazelcast.internal.query.sort.SortKey;
import com.hazelcast.internal.query.sort.SortKeyComparator;

import java.util.ArrayList;
import java.util.List;

public class SortMergeReceiveExec extends AbstractExec {
    /** Inbox to consume results from. */
    private final StripedInbox inbox;

    /** Expressions. */
    private final List<Expression> expressions;

    /** Input stripes. */
    private final List<Row>[] stripes;

    /** Finished stripes. */
    private final boolean[] stripesDone;

    /** Comparator. */
    private final SortKeyComparator comparator;

    /** Current batch. */
    private RowBatch curBatch;

    /** Whether all sources are available for sorting. */
    private boolean inputsAvailable;

    @SuppressWarnings("unchecked")
    public SortMergeReceiveExec(StripedInbox inbox, List<Expression> expressions, List<Boolean> ascs) {
        this.inbox = inbox;
        this.expressions = expressions;

        // If there is only one input edge, then normal ReceiveExec should be used instead.
        assert inbox.getStripeCount() > 1;

        stripes = new List[inbox.getStripeCount()];
        stripesDone = new boolean[inbox.getStripeCount()];

        // TODO: Avoid wrapping into SortKey.
        comparator = new SortKeyComparator(ascs);
    }

    @Override
    public IterationResult advance() {
        // Try polling inputs.
        if (!pollInputs())
            return inbox.closed() ? IterationResult.FETCHED_DONE : IterationResult.WAIT;

        // All inputs available, sort as much as possible.
        prepareBatch();

        return inbox.closed() ? IterationResult.FETCHED_DONE : IterationResult.FETCHED;
    }

    @Override
    public RowBatch currentBatch() {
        return curBatch;
    }

    /**
     * Try polling inputs so that at least one batch is available everywhere.
     *
     * @return {@code True} if all batches are available.
     */
    private boolean pollInputs() {
        if (inputsAvailable)
            return true;

        boolean res = true;

        for (int i = 0; i < stripes.length; i++) {
            if (stripesDone[i])
                continue;

            List<Row> stripeRows = stripes[i];

            if (stripeRows == null) {
                while (true) {
                    SendBatch stripeBatch = inbox.poll(i);

                    if (stripeBatch == null) {
                        // No batch available at the moment, wait.
                        res = false;

                        break;
                    }
                    else {
                        if (stripeBatch.isLast())
                            stripesDone[i] = true;

                        List<Row> rows = stripeBatch.getRows();

                        if (!rows.isEmpty()) {
                            stripes[i] = rows;

                            break;
                        }
                    }
                }
            }
        }

        if (res)
            inputsAvailable = true;

        return res;
    }

    /**
     * Prepare sorted batch.
     */
    private void prepareBatch() {
        List<Row> rows = new ArrayList<>();

        // Sort entries until inputs are available.
        while (inputsAvailable) {
            SortKey curKey = null;
            int curIdx = -1;

            for (int i = 0; i < stripes.length; i++) {
                List<Row> stripeRows = stripes[i];

                if (stripeRows == null) {
                    assert stripesDone[i];

                    continue;
                }

                assert !stripeRows.isEmpty();

                SortKey stripeKey = prepareSortKey(stripeRows.get(0), i);

                if (curKey == null || comparator.compare(stripeKey, curKey) < 0) {
                    curKey = stripeKey;
                    curIdx = i;
                }
            }

            if (curKey == null)
                // Avoid infinite loop is all stripes are done.
                break;
            else {
                List<Row> stripeRows = stripes[curIdx];

                rows.add(stripeRows.remove(0));

                if (stripeRows.isEmpty()) {
                    stripes[curIdx] = null;

                    if (!stripesDone[curIdx])
                        inputsAvailable = false;
                }
            }
        }

        curBatch = new ListRowBatch(rows);
    }

    /**
     * Prepare sort key for the row.
     *
     * @param row Row.
     * @param stripe Source stripe.
     * @return Key.
     */
    private SortKey prepareSortKey(Row row, int stripe) {
        List<Object> key = new ArrayList<>(expressions.size());

        for (Expression expression : expressions)
            key.add(expression.eval(ctx, row));

        return new SortKey(key, stripe);
    }
}
