/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.sql.impl.row.Row;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Utility class to perform merge sort with min-heap.
 */
public class MergeSort {
    /** Constant for unlimited number of rows. */
    public static final int UNLIMITED = 0;

    /** Sources. */
    private final MergeSortSource[] sources;

    // TODO: Heap might be an overkill for small number of inputs (2, 3, 4?). Consider other implementations.
    /** Current items. */
    private final PriorityQueue<SortKey> heap;

    /** Optional limit on the number of returned results. */
    private final int limit;

    /** Sources which are not in the heap yet. */
    private final Set<Integer> missingSourceIndexes = new HashSet<>();

    /** Number of returned rows. */
    private int returnedCount;

    /** Whether the sorting is finished. */
    private boolean done;

    public MergeSort(MergeSortSource[] sources, SortKeyComparator comparator) {
        this(sources, comparator, UNLIMITED);
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is an internal class")
    public MergeSort(MergeSortSource[] sources, SortKeyComparator comparator, int limit) {
        this.sources = sources;
        this.heap = new PriorityQueue<>(comparator);
        this.limit = limit > 0 ? limit : UNLIMITED;

        for (int i = 0; i < sources.length; i++) {
            missingSourceIndexes.add(i);
        }
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
     * @return {@code True} if all sources are fetched and sorting can proceed.
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
     * Fetch rows.
     *
     * @return Fetched rows.
     */
    private List<Row> fetch() {
        assert missingSourceIndexes.isEmpty();

        List<Row> rows = new ArrayList<>();

        while (true) {
            if (heap.isEmpty() || (limit > 0 && returnedCount == limit)) {
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

            rows.add(row);

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

            returnedCount++;
        }
    }

    public boolean isDone() {
        return done;
    }
}
