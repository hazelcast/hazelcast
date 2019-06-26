package com.hazelcast.internal.query.sort;

import java.util.Comparator;
import java.util.List;

public class SortKeyComparator implements Comparator<SortKey> {

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
            // TODO: Comparable checks.
            // TODO: Proper NULL handling.
            // TODO: Proper collation and type comparisons!
            Comparable item1Comp = (Comparable)item1;
            Comparable item2Comp = (Comparable)item2;

            res = item1Comp.compareTo(item2Comp);

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
