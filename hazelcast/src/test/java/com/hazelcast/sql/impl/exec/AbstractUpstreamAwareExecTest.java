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
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractUpstreamAwareExecTest extends SqlTestSupport {
    @Test
    public void testExec() {
        ChildExec childExec = new ChildExec();
        ParentExec parentExec = new ParentExec(childExec);

        QueryFragmentContext context = emptyFragmentContext();

        parentExec.setup(context);

        assertSame(context, childExec.context);
        assertSame(context, parentExec.context);
    }

    private static final class ChildExec extends AbstractExec {

        private QueryFragmentContext context;

        private ChildExec() {
            super(1);
        }

        @Override
        protected void setup0(QueryFragmentContext context) {
            this.context = context;
        }

        @Override
        protected IterationResult advance0() {
            return null;
        }

        @Override
        protected RowBatch currentBatch0() {
            return null;
        }
    }

    private static final class ParentExec extends AbstractUpstreamAwareExec {

        private QueryFragmentContext context;

        private ParentExec(Exec upstream) {
            super(2, upstream);
        }

        @Override
        protected void setup1(QueryFragmentContext context) {
            this.context = context;
        }

        @Override
        protected IterationResult advance0() {
            return null;
        }

        @Override
        protected RowBatch currentBatch0() {
            return null;
        }
    }
}
