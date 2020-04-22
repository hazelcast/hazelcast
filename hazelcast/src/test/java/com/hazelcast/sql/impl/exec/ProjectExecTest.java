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
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProjectExecTest extends SqlTestSupport {
    @Test
    public void testProject() {
        UpstreamExec upstream = new UpstreamExec(1);
        ProjectExec exec = createExec(upstream);

        // Test empty.
        assertEquals(IterationResult.WAIT, exec.advance());

        upstream.addResult(IterationResult.FETCHED, EmptyRowBatch.INSTANCE);
        assertEquals(IterationResult.WAIT, exec.advance());

        // Test not last batch.
        upstream.addResult(IterationResult.FETCHED, createMonotonicBatch(0, 100));
        assertEquals(IterationResult.FETCHED, exec.advance());
        checkBatch(exec.currentBatch(), 0, 100);

        // Test last batch.
        upstream.addResult(IterationResult.FETCHED_DONE, createMonotonicBatch(100, 100));
        assertEquals(IterationResult.FETCHED_DONE, exec.advance());
        checkBatch(exec.currentBatch(), 100, 100);

        // Test empty last batch.
        upstream = new UpstreamExec(1);
        exec = createExec(upstream);

        upstream.addResult(IterationResult.FETCHED, createMonotonicBatch(0, 100));
        assertEquals(IterationResult.FETCHED, exec.advance());
        checkBatch(exec.currentBatch(), 0, 100);

        upstream.addResult(IterationResult.FETCHED_DONE, EmptyRowBatch.INSTANCE);
        assertEquals(IterationResult.FETCHED_DONE, exec.advance());
        assertEquals(0, exec.currentBatch().getRowCount());
    }

    @SuppressWarnings("rawtypes")
    private static ProjectExec createExec(UpstreamExec upstream) {
        ColumnExpression<?> expression = ColumnExpression.create(0, QueryDataType.INT);
        List<Expression> projects = Arrays.asList(expression, expression);

        ProjectExec exec = new ProjectExec(2, upstream, projects);
        exec.setup(emptyFragmentContext());

        return exec;
    }

    private static void checkBatch(RowBatch batch, int startValue, int size) {
        assertEquals(size, batch.getRowCount());

        for (int i = 0; i < size; i++) {
            int value0 = batch.getRow(i).get(0);
            int value1 = batch.getRow(i).get(1);

            assertEquals(startValue + i, value0);
            assertEquals(startValue + i, value1);
        }
    }
}
