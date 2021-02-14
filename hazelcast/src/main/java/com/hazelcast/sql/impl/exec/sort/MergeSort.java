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

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import static com.hazelcast.sql.impl.exec.fetch.Fetch.getFetchValue;
import static com.hazelcast.sql.impl.exec.fetch.Fetch.getOffsetValue;

/**
 * An utility class to perform merge sort with min-heap.
 */
@SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "This is an internal class")
public class MergeSort {

    /**
     * Sources.
     */
    private final MergeSortSource[] sources;

    /**
     * Current items.
     */
    private final PriorityQueue<SortKey> heap;

    /**
     * Optional limit on the number of returned results.
     */
    private long fetchValue;

    /**
     * Optional offset value for the results.
     */
    private long offsetValue;

    /**
     * Sources which are not in the heap yet.
     */
    private final Set<Integer> missingSourceIndexes = new HashSet<>();

    /**
     * Fetch expression.
     */
    private final Expression<?> fetch;

    /**
     * Offset expression
     */
    private final Expression<?> offset;
    /**
     * Number of returned rows.
     */
    private long returnedCount;

    /**
     * The offset value applied to the result rows.
     */
    private int offsetApplied;

    /**
     * Whether the sorting is finished.
     */
    private boolean done;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is an internal class")
    public MergeSort(MergeSortSource[] sources, SortKeyComparator comparator, Expression<?> fetch,
                     Expression<?> offset) {
        this.sources = sources;
        this.heap = new PriorityQueue<>(comparator);
        this.fetch = fetch;
        this.offset = offset;

        for (int i = 0; i < sources.length; i++) {
            missingSourceIndexes.add(i);
        }
    }

    public void setup(ExpressionEvalContext context) {
        fetchValue = getFetchValue(context, fetch);
        offsetValue = getOffsetValue(context, offset);
    }

    public List<Row> nextBatch() {
        // Do not return more rows if we are done.
        if (done) {
            return null;
        }

        if (!prepare()) {
            return null;
        }

        return fetch();
    }

    /**
     * Prepare the heap.
     *
     * @return {@code true} if all sources are fetched and sorting can proceed.
     */
    private boolean prepare() {
        if (missingSourceIndexes.isEmpty()) {
            return true;
        }

        Iterator<Integer> iter = missingSourceIndexes.iterator();

        while (iter.hasNext()) {
            Integer index = iter.next();

            MergeSortSource source = sources[index];

            if (source.advance()) {
                SortKey key = source.peekKey();

                assert key != null;

                heap.add(key);

                iter.remove();
            } else {
                if (source.isDone()) {
                    iter.remove();
                } else {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Fetch a batch of rows.
     *
     * @return Fetched rows.
     */
    private List<Row> fetch() {
        assert missingSourceIndexes.isEmpty();

        List<Row> rows = new ArrayList<>();

        while (true) {
            if (heap.isEmpty() || returnedCount == fetchValue) {
                done = true;

                return rows;
            }

            // Get current row.
            SortKey key = heap.poll();

            assert key != null;

            int sourceIndex = (int) key.getIndex();

            MergeSortSource source = sources[sourceIndex];

            Row row = source.peekRow();

            assert row != null;

            if (offsetApplied >= offsetValue) {
                rows.add(row);
                returnedCount++;
            } else {
                offsetApplied++;
            }

            // Put the next value to heap or stop if no more data is available.
            if (source.advance()) {
                SortKey nextKey = source.peekKey();

                heap.add(nextKey);
            } else {
                if (!source.isDone()) {
                    missingSourceIndexes.add(sourceIndex);

                    return rows;
                }
            }
        }
    }

    public boolean isDone() {
        return done;
    }

    // For unit testing only
    public MergeSortSource[] getSources() {
        return sources;
    }
}
