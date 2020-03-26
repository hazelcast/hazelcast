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

import com.hazelcast.sql.impl.fragment.QueryFragmentContext;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.RowBatch;
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
public class AbstractExecTest {
    @Test
    public void testPropagation() {
        TestExec exec = new TestExec(1);

        // ID.
        assertEquals(1, exec.getId());

        // Setup.
        QueryFragmentContext context = new QueryFragmentContext(Collections.emptyList(), null, null);

        exec.setup(context);

        assertSame(context, exec.propagatedContext);

        // Advance management
        for (IterationResult result : IterationResult.values()) {
            exec.currentResult = result;

            assertEquals(result, exec.advance());
        }

        // Batch management.
        assertSame(EmptyRowBatch.INSTANCE, exec.currentBatch());

        exec.currentBatch = new ListRowBatch();

        assertSame(exec.currentBatch, exec.currentBatch());
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
