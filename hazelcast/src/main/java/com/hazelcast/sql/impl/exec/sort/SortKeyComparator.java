/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.exec.sort;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Comparator;

/**
 * Comparator fot the sort key.
 */
@SuppressFBWarnings({"SE_COMPARATOR_SHOULD_BE_SERIALIZABLE", "EI_EXPOSE_REP2"})
public class SortKeyComparator implements Comparator<SortKey> {
    /**
     * An array of ascending collations.
     */
    private final boolean[] ascs;

    public SortKeyComparator(boolean[] ascs) {
        this.ascs = ascs;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public int compare(SortKey o1, SortKey o2) {
        Object[] key1 = o1.getKey();
        Object[] key2 = o2.getKey();

        // We use the default ordering for the null values, that is
        // null value is LESS than any other non-null value
        for (int i = 0; i < ascs.length; i++) {
            boolean asc = ascs[i];

            Object item1 = key1[i];
            Object item2 = key2[i];

            Comparable item1Comp = (Comparable) item1;
            Comparable item2Comp = (Comparable) item2;

            int res = compare(item1Comp, item2Comp, asc);

            if (res != 0) {
                return res;
            }
        }

        return Long.compare(o1.getIndex(), o2.getIndex());
    }

    private int compare(Comparable o1, Comparable o2, boolean asc) {
        if (o1 == null && o2 == null) {
            return 0;
        }

        if (o1 == null) {
            return asc ? -1 : 1;
        }

        if (o2 == null) {
            return asc ? 1 : -1;
        }

        return asc ? o1.compareTo(o2) : o2.compareTo(o1);
    }

}
