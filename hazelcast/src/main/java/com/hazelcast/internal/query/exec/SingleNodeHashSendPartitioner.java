package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.internal.query.io.Row;

/**
 * Partitioner which sends a row to a single node based on hash value.
 */
public class SingleNodeHashSendPartitioner implements SendPartitioner {

    private final Outbox[] outboxes;
    private final Expression[] keyExpressions;

    public SingleNodeHashSendPartitioner(Outbox[] outboxes, Expression[] keyExpressions) {
        this.outboxes = outboxes;
        this.keyExpressions = keyExpressions;
    }

    @Override
    public Outbox map(QueryContext ctx, Row row) {
        int hash = 0;

        if (keyExpressions == null) {
            for (int i = 0; i < row.getColumnCount(); i++)
                hash = hash(hash, row.getColumn(i));
        }
        else {
            for (Expression keyExpression : keyExpressions)
                hash = hash(hash, keyExpression.eval(ctx, row));
        }

        return outboxes[hash % outboxes.length];
    }

    private static int hash(int hash, Object nextObject) {
        return 31 * hash + (nextObject != null ? nextObject.hashCode() : 0);
    }
}
