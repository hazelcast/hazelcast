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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.exec.root.RootExec;
import com.hazelcast.sql.impl.exec.root.RootResultConsumer;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.plan.node.PlanNodeVisitor;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CreateExecPlanNodeVisitorTest {

    @Test
    public void testRoot() {
        int upstreamId = 1;
        int rootId = 2;

        TestRootResultConusmer consumer = new TestRootResultConusmer();

        TestUpstreamNode upstreamNode = new TestUpstreamNode(upstreamId);
        RootPlanNode rootNode = new RootPlanNode(rootId, upstreamNode);

        CreateExecPlanNodeVisitor visitor =
            new CreateExecPlanNodeVisitor(null, new QueryExecuteOperation().setRootConsumer(consumer, 1000), null, null);

        rootNode.visit(visitor);

        RootExec rootExec = (RootExec) visitor.getExec();
        assertEquals(rootId, rootExec.getId());

        TestUpstreamExec upstreamExec = (TestUpstreamExec) rootExec.getUpstream();
        assertEquals(upstreamId, upstreamExec.getId());
    }

    private static class TestUpstreamExec extends AbstractExec {
        private TestUpstreamExec(int id) {
            super(id);
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

    private static class TestUpstreamNode implements PlanNode, CreateExecPlanNodeVisitorCallback {

        private final int id;

        private TestUpstreamNode(int id) {
            this.id = id;
        }

        @Override
        public int getId() {
            return id;
        }

        @Override
        public void visit(PlanNodeVisitor visitor) {
            visitor.onOtherNode(this);
        }

        @Override
        public void onVisit(CreateExecPlanNodeVisitor visitor) {
            visitor.push(new TestUpstreamExec(getId()));
        }

        @Override
        public PlanNodeSchema getSchema() {
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            // No-op.
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            // No-op.
        }
    }

    private static class TestRootResultConusmer implements RootResultConsumer {
        @Override
        public void setup(QueryFragmentContext context) {
            // No-op.
        }

        @Override
        public boolean consume(List<Row> batch, boolean last) {
            return false;
        }

        @Override
        public Iterator<Row> iterator() {
            return null;
        }

        @Override
        public void onError(HazelcastSqlException error) {
            // No-op.
        }
    }
}
