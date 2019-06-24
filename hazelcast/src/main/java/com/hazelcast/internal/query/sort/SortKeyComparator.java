package com.hazelcast.internal.query.sort;

import com.hazelcast.internal.query.QueryUtils;

import java.util.Comparator;
import java.util.List;

public class SortKeyComparator implements Comparator<SortKey> {
    // TODO: Collation
    // TODO: NULLS FIRST/LAST
    // TODO: Type inference

    private final List<Boolean> ascs;

    public SortKeyComparator(List<Boolean> ascs) {
        this.ascs = ascs;
    }

    @Override
    public int compare(SortKey o1, SortKey o2) {
        int res;

        for (int i = 0; i < ascs.size(); i++) {
            boolean asc = ascs.get(i);

            Object item1 = o1.getKey().get(i);
            Object item2 = o2.getKey().get(i);

            // TODO: ANSI-compliant sort rules.
            res = QueryUtils.compare(item1, item2);

            if (!asc)
                res = -res;

            if (res != 0) {
                if (!asc)
                    res = -res;

                return res;
            }
        }

        return Long.compare(o1.getIdx(), o2.getIdx());
    }
}
