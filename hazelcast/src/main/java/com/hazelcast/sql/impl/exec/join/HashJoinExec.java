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

package com.hazelcast.sql.impl.exec.join;

import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.sql.impl.exec.AbstractUpstreamAwareExec;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.exec.UpstreamState;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.JoinRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

/**
 * Hash join implementation: build hash table from the right input, do lookups for the left one.
 */
// TODO: Semi join support
public class HashJoinExec extends AbstractUpstreamAwareExec {
    /** Right input. */
    private final UpstreamState rightState;

    /** Filter. */
    // TODO: Currently this condition includes equality checks for hash keys. They are unnecessary. Remove during planning stage.
    private final Expression<Boolean> filter;

    /** Left hash keys. */
    private final List<Integer> leftHashKeys;

    /** Right hash keys. */
    private final List<Integer> rightHashKeys;

    /** Whether this is the outer join. */
    private final boolean outer;

    /** Whether this is the semi join. */
    private final boolean semi;

    /** Empty right row. */
    private final Row rightEmptyRow;

    /** Current left row. */
    private Row leftRow;

    /** Current matching right rows. */
    private List<Row> rightRows;

    /** Position in the right row batch. */
    private int rightRowPos;

    /** Hash table for the right input. */
    private final HashMap<Object, List<Row>> table = new HashMap<>();

    /** Current row. */
    private RowBatch curRow;

    public HashJoinExec(
        int id,
        Exec left,
        Exec right,
        Expression<Boolean> filter,
        List<Integer> leftHashKeys,
        List<Integer> rightHashKeys,
        boolean outer,
        boolean semi,
        int rightRowColumnCount
    ) {
        super(id, left);

        rightState = new UpstreamState(right);

        this.filter = filter;
        this.leftHashKeys = leftHashKeys;
        this.rightHashKeys = rightHashKeys;

        this.outer = outer;
        this.semi = semi;

        rightEmptyRow = outer ? new HeapRow(rightRowColumnCount) : null;
    }

    @Override
    protected void setup1(QueryFragmentContext ctx) {
        rightState.setup(ctx);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    @Override
    public IterationResult advance0() {
        // Build hash table for the right input.
        while (!rightState.isDone()) {
            if (rightState.advance()) {
                for (Row row : rightState) {
                    add(row);
                }
            } else {
                return IterationResult.WAIT;
            }
        }

        // Special case: right input produced no results. Should not be triggered for the outer join.
        if (table.isEmpty() && !outer) {
            curRow = null;

            return IterationResult.FETCHED_DONE;
        }

        // Main join cycle.
        while (true) {
            // Loop until we find a match between the left and right inputs.
            while (leftRow == null) {
                // Check if we have reached the end.
                if (state.isDone()) {
                    curRow = null;

                    return IterationResult.FETCHED_DONE;
                }

                // Get the next data chunk.
                if (!state.advance()) {
                    return IterationResult.WAIT;
                }

                // For every left row ...
                for (Row leftRow0 : state) {
                    // ... get matching right rows.
                    List<Row> rightRows0 = get(leftRow0);

                    // If there are matching right rows, then move to the next step where we actually produce the join rows.
                    // We also switch to the next step for outer join, to produce [left, null] row.
                    // Otherwise, switch to the next left row.
                    if (!rightRows0.isEmpty() || outer) {
                        leftRow = leftRow0;
                        rightRows = rightRows0;
                        rightRowPos = 0;

                        break;
                    }
                }
            }

            // We have a specific left row and a set of matching right rows. Match them.
            boolean found = false;

            for (int i = rightRowPos; i < rightRows.size(); i++) {
                Row rightRow = rightRows.get(i);

                JoinRow row = new JoinRow(leftRow, rightRow);

                if (filter.eval(row, ctx)) {
                    rightRowPos = i + 1;

                    curRow = row;

                    found = true;

                    break;
                }
            }

            // Reset the left row if there is no match of we reached the end of the matching right input.
            if (!found || rightRowPos == rightRows.size()) {
                if (outer && rightRows.isEmpty()) {
                    // But do not forget to emit the [left, null] pair for the outer join if there we no match.
                    curRow = new JoinRow(leftRow, rightEmptyRow);

                    found = true;
                }

                leftRow = null;
            }

            if (found) {
                // If a single match was found, and this is the semi join, then nullify the left row, so no more pairs for this
                // row is produced.
                if (semi) {
                    leftRow = null;
                }

                return IterationResult.FETCHED;
            }
        }
    }

    @Override
    public RowBatch currentBatch0() {
        return curRow;
    }

    @Override
    public boolean canReset() {
        return super.canReset() && rightState.canReset();
    }

    @Override
    protected void reset1() {
        rightState.reset();

        leftRow = null;
        rightRows = null;
        rightRowPos = 0;
        table.clear();

        curRow = null;
    }

    /**
     * Add row from the right input to the hash table.
     *
     * @param rightRow Row.
     */
    private void add(Row rightRow) {
        Object key = prepareKey(rightRow, rightHashKeys);

        List<Row> rows = table.computeIfAbsent(key, k -> new ArrayList<>(1));

        rows.add(rightRow);
    }

    /**
     * Get matching values from the right input for the left row.
     *
     * @param leftRow Left row.
     * @return Matching right rows.
     */
    private List<Row> get(Row leftRow) {
        Object key = prepareKey(leftRow, leftHashKeys);

        List<Row> rows = table.get(key);

        return rows != null ? rows : Collections.emptyList();
    }

    /**
     * Prepare hash key for the row.
     *
     * @param row Row.
     * @param hashKeys Columns to be used for hash key.
     * @return Key.
     */
    private Object prepareKey(Row row, List<Integer> hashKeys) {
        if (hashKeys.size() == 1) {
            return row.get(hashKeys.get(0));
        } else if (hashKeys.size() == 2) {
            return new HashKey2(
                row.get(hashKeys.get(0)),
                row.get(hashKeys.get(1))
            );
        } else {
            Object[] vals = new Object[hashKeys.size()];

            for (int i = 0; i < hashKeys.size(); i++) {
                vals[i] = row.get(hashKeys.get(i));
            }

            return new HashKeyN(vals);
        }
    }

    private static final class HashKey2 {
        private final Object val1;
        private final Object val2;

        private HashKey2(Object val1, Object val2) {
            this.val1 = val1;
            this.val2 = val2;
        }

        @Override
        public int hashCode() {
            int res = val1 != null ? val1.hashCode() : 0;

            res = 31 * res + (val2 != null ? val2.hashCode() : 0);

            return res;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            HashKey2 hashKey2 = (HashKey2) o;

            return Objects.equals(val1, hashKey2.val1) && Objects.equals(val2, hashKey2.val2);
        }
    }

    private static final class HashKeyN {
        private final Object[] vals;

        private HashKeyN(Object[] vals) {
            this.vals = vals;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            HashKeyN hashKeyN = (HashKeyN) o;

            return Arrays.equals(vals, hashKeyN.vals);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(vals);
        }
    }
}
