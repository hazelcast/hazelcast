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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.UpstreamExec;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.FunctionalPredicateExpression;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.exec.AbstractFilterExec.BATCH_SIZE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FilterExecTest extends SqlTestSupport {
    @Test
    public void testFilter() {
        UpstreamExec upstream = new UpstreamExec(1);
        Expression<Boolean> filter = new FunctionalPredicateExpression((row) -> {
            int val = row.get(0);

            if (val % 2 == 0) {
                return true;
            } else if ((val / 2) % 2 == 0) {
                return false;
            } else {
                return null;
            }
        });

        FilterExec exec = new FilterExec(2, upstream, filter);
        exec.setup(emptyFragmentContext());

        // Test empty state.
        assertEquals(IterationResult.WAIT, exec.advance());

        upstream.addResult(IterationResult.FETCHED, EmptyRowBatch.INSTANCE);
        assertEquals(IterationResult.WAIT, exec.advance());

        // Consume several batches, still insufficient to produce a result.
        upstream.addResult(IterationResult.FETCHED, createMonotonicBatch(0, BATCH_SIZE / 2));
        upstream.addResult(IterationResult.FETCHED, createMonotonicBatch(BATCH_SIZE / 2, BATCH_SIZE / 2));
        upstream.addResult(IterationResult.FETCHED, createMonotonicBatch(BATCH_SIZE, BATCH_SIZE / 2));

        assertEquals(IterationResult.WAIT, exec.advance());

        // One more batch, finally producing some rows.
        upstream.addResult(IterationResult.FETCHED, createMonotonicBatch(3 * BATCH_SIZE / 2, 3 * BATCH_SIZE / 2));

        assertEquals(IterationResult.FETCHED, exec.advance());
        checkBatch(exec.currentBatch(), 0, BATCH_SIZE);

        // One more batch to finalize the results.
        upstream.addResult(IterationResult.FETCHED_DONE, createMonotonicBatch(3 * BATCH_SIZE, BATCH_SIZE * 2));

        assertEquals(IterationResult.FETCHED, exec.advance());
        checkBatch(exec.currentBatch(), 2 * BATCH_SIZE, BATCH_SIZE);

        assertEquals(IterationResult.FETCHED_DONE, exec.advance());
        checkBatch(exec.currentBatch(), 4 * BATCH_SIZE, BATCH_SIZE / 2);
    }

    private static void checkBatch(RowBatch batch, int startValue, int size) {
        assertEquals(size, batch.getRowCount());

        for (int i = 0; i < size; i++) {
            int value = batch.getRow(i).get(0);

            assertEquals(startValue + (i * 2), value);
        }
    }
}
