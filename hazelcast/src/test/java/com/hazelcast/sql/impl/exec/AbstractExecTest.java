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

import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.state.QueryStateCallback;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractExecTest extends SqlTestSupport {
    @Test
    public void testPropagation() {
        TestExec exec = new TestExec(1);

        // ID.
        assertEquals(1, exec.getId());

        // Setup.
        QueryFragmentContext context = emptyFragmentContext();

        exec.setup(context);

        assertSame(context, exec.propagatedContext);

        // Advance management.
        for (IterationResult result : IterationResult.values()) {
            if (result == IterationResult.FETCHED_DONE) {
                continue;
            }

            exec.currentResult = result;
            assertEquals(result, exec.advance());
        }

        // Check done state.
        exec.currentResult = IterationResult.FETCHED_DONE;
        assertEquals(IterationResult.FETCHED_DONE, exec.advance());
        assertThrows(IllegalStateException.class, exec::advance);

        // Batch management.
        assertSame(EmptyRowBatch.INSTANCE, exec.currentBatch());

        exec.currentBatch = new ListRowBatch();

        assertSame(exec.currentBatch, exec.currentBatch());
    }

    @Test
    public void testCancel() {
        QueryStateCallback stateCallback = new QueryStateCallback() {
            @Override
            public void onFragmentFinished() {
                // No-op.
            }

            @Override
            public void cancel(Exception e) {
                // No-op.
            }

            @Override
            public void checkCancelled() {
                throw QueryException.cancelledByUser();
            }
        };

        TestExec exec = new TestExec(1);
        exec.setup(new QueryFragmentContext(Collections.emptyList(), null, stateCallback));

        QueryException error = assertThrows(QueryException.class, exec::advance);
        assertEquals(SqlErrorCode.CANCELLED_BY_USER, error.getCode());
    }

    private static final class TestExec extends AbstractExec {

        private QueryFragmentContext propagatedContext;
        private IterationResult currentResult;
        private RowBatch currentBatch;

        private TestExec(int id) {
            super(id);
        }

        @Override
        protected void setup0(QueryFragmentContext ctx) {
            propagatedContext = ctx;
        }

        @Override
        protected IterationResult advance0() {
            return currentResult;
        }

        @Override
        protected RowBatch currentBatch0() {
            return currentBatch;
        }
    }
}
