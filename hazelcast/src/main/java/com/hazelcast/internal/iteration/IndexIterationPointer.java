package com.hazelcast.internal.iteration;

import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexInFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.util.ArrayList;
import java.util.List;

// TODO add IdentifiedDataSerializable
public class IndexIterationPointer {

    private static final int FLAG_DESCENDING = 1;
    private static final int FLAG_FROM_INCLUSIVE = 2;
    private static final int FLAG_TO_INCLUSIVE = 4;

    private Comparable<?> from;
    private Comparable<?> to;
    private byte flags;

    public IndexIterationPointer(Comparable<?> from, boolean fromInclusive, Comparable<?> to, boolean toInclusive, boolean descending) {
        this.from = from;
        this.to = to;

        flags = (byte) ((descending ? FLAG_DESCENDING : 0)
                | (fromInclusive ? FLAG_FROM_INCLUSIVE : 0)
                | (toInclusive ? FLAG_TO_INCLUSIVE : 0));
    }

    public static IndexIterationPointer[] createFromIndexFilter(IndexFilter indexFilter, ExpressionEvalContext evalContext) {
        ArrayList<IndexIterationPointer> result = new ArrayList<>();
        createFromIndexFilterInt(indexFilter, evalContext, result);
        return result.toArray(new IndexIterationPointer[0]);
    }

    private static void createFromIndexFilterInt(
            IndexFilter indexFilter,
            ExpressionEvalContext evalContext,
            List<IndexIterationPointer> result
    ) {
        if (indexFilter instanceof IndexRangeFilter) {
            IndexRangeFilter rangeFilter = (IndexRangeFilter) indexFilter;
            Comparable<?> from = rangeFilter.getFrom().getValue(evalContext);
            Comparable<?> to = rangeFilter.getTo().getValue(evalContext);
            result.add(new IndexIterationPointer(from, rangeFilter.isFromInclusive(), to, rangeFilter.isToInclusive(), false));
        } else if (indexFilter instanceof IndexEqualsFilter) {
            IndexEqualsFilter equalsFilter = (IndexEqualsFilter) indexFilter;
            Comparable<?> value = equalsFilter.getComparable(evalContext);
            result.add(new IndexIterationPointer(value, true, value, true, false));
        } else if (indexFilter instanceof IndexInFilter) {
            IndexInFilter inFilter = (IndexInFilter) indexFilter;
            for (IndexFilter filter : inFilter.getFilters()) {
                createFromIndexFilterInt(filter, evalContext, result);
            }
        }
    }

    public Comparable<?> getFrom() {
        return from;
    }

    public boolean isFromInclusive() {
        return (flags & FLAG_FROM_INCLUSIVE) != 0;
    }

    public Comparable<?> getTo() {
        return to;
    }

    public boolean isToInclusive() {
        return (flags & FLAG_TO_INCLUSIVE) != 0;
    }

    public boolean isDescending() {
        return (flags & FLAG_DESCENDING) != 0;
    }
}
