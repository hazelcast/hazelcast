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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UpstreamStateTest extends SqlTestSupport {
    @Test
    public void testUpstreamState() {
        List<Row> rows1 = new ArrayList<>();
        rows1.add(new HeapRow(1));
        rows1.add(new HeapRow(1));

        RowBatch batch = new ListRowBatch(rows1);

        UpstreamExec exec = new UpstreamExec();
        UpstreamState state = new UpstreamState(exec);

        // Test setup.
        QueryFragmentContext context = emptyFragmentContext();

        state.setup(context);

        assertSame(context, exec.context);

        // Test advance on non-consumed and consumed states.
        exec.update(IterationResult.FETCHED, batch);

        assertEquals(EmptyRowBatch.INSTANCE, state.consumeBatch());

        assertTrue(state.advance());
        assertEquals(1, exec.advanceCalled);
        assertTrue(state.advance());
        assertEquals(1, exec.advanceCalled);
        assertFalse(state.isDone());

        // Get data through iterator.
        assertTrue(state.iterator().hasNext());
        assertSame(rows1.get(0), state.iterator().next());
        assertFalse(state.isDone());
        checkCannotConsume(state);
        assertSame(rows1.get(1), state.nextIfExists());
        assertFalse(state.isDone());
        assertFalse(state.iterator().hasNext());
        assertNull(state.nextIfExists());
        checkCannotConsume(state);
        assertFalse(state.isDone());

        // Check WAIT.
        exec.update(IterationResult.WAIT, EmptyRowBatch.INSTANCE);

        assertFalse(state.advance());
        assertFalse(state.iterator().hasNext());
        assertNull(state.nextIfExists());
        assertSame(EmptyRowBatch.INSTANCE, state.consumeBatch());

        // Get data through batch.
        exec.update(IterationResult.FETCHED_DONE, batch);

        assertTrue(state.advance());
        assertEquals(1, exec.advanceCalled);
        assertSame(batch, state.consumeBatch());
        assertFalse(state.iterator().hasNext());
        assertNull(state.nextIfExists());
        checkCannotConsume(state);
        assertTrue(state.isDone());

        // Check interaction with already completed upstream.
        assertTrue(state.advance());
        assertEquals(1, exec.advanceCalled);
    }

    private void checkCannotConsume(UpstreamState state) {
        try {
            state.consumeBatch();

            fail("Must fail");
        } catch (QueryException e) {
            // No-op.
        }
    }

    private static final class UpstreamExec extends AbstractExec {

        private IterationResult currentResult;
        private RowBatch currentBatch;
        private QueryFragmentContext context;
        private int advanceCalled;

        private UpstreamExec() {
            super(1);
        }

        @Override
        protected void setup0(QueryFragmentContext context) {
            this.context = context;
        }

        private void update(IterationResult currentResult, RowBatch currentBatch) {
            this.currentResult = currentResult;
            this.currentBatch = currentBatch;

            advanceCalled = 0;
        }

        @Override
        protected IterationResult advance0() {
            advanceCalled++;

            return currentResult;
        }

        @Override
        protected RowBatch currentBatch0() {
            return currentBatch;
        }
    }
}
