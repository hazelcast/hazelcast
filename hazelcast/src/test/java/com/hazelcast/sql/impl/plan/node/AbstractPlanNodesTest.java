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

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractPlanNodesTest {
    @Test
    public void testZeroInputPlanNode() {
        // Test fields.
        int id = 1;
        List<QueryDataType> fieldTypes = Collections.singletonList(QueryDataType.INT);

        TestZeroInputPlanNode node = new TestZeroInputPlanNode(id, fieldTypes);

        assertEquals(id, node.getId());
        assertEquals(fieldTypes, node.getSchema().getTypes());

        // Test serialization.
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        TestZeroInputPlanNode restoredNode = ss.toObject(ss.toData(node));

        assertEquals(id, restoredNode.getId());
        assertEquals(fieldTypes, restoredNode.getSchema().getTypes());
    }

    @Test
    public void testUniInputPlanNode() {
        // Test fields.
        int upstreamId = 1;
        List<QueryDataType> upstreamFieldTypes = Collections.singletonList(QueryDataType.INT);
        TestZeroInputPlanNode upstreamNode = new TestZeroInputPlanNode(upstreamId, upstreamFieldTypes);

        int id = 2;
        List<QueryDataType> fieldTypes = Collections.singletonList(QueryDataType.BIGINT);
        TestUniInputPlanNode node = new TestUniInputPlanNode(id, upstreamNode, fieldTypes);

        assertEquals(id, node.getId());
        assertEquals(fieldTypes, node.getSchema().getTypes());

        // Test serialization.
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        TestUniInputPlanNode restoredNode = ss.toObject(ss.toData(node));
        assertEquals(id, restoredNode.getId());
        assertEquals(fieldTypes, restoredNode.getSchema().getTypes());

        TestZeroInputPlanNode restoredUpstreamNode = (TestZeroInputPlanNode) restoredNode.getUpstream();
        assertEquals(upstreamId, restoredUpstreamNode.getId());
        assertEquals(upstreamFieldTypes, restoredUpstreamNode.getSchema().getTypes());

        // Test visitor.
        TestPlanNodeVisitor visitor = new TestPlanNodeVisitor();

        node.visit(visitor);

        assertEquals(2, visitor.getNodes().size());
        assertEquals(upstreamId, visitor.getNodes().get(0).getId());
        assertEquals(id, visitor.getNodes().get(1).getId());
    }

    private static class TestZeroInputPlanNode extends ZeroInputPlanNode {

        private List<QueryDataType> fieldTypes;

        private TestZeroInputPlanNode() {
            // No-op.
        }

        private TestZeroInputPlanNode(int id, List<QueryDataType> fieldTypes) {
            super(id);

            this.fieldTypes = fieldTypes;
        }

        @Override
        protected PlanNodeSchema getSchema0() {
            return new PlanNodeSchema(fieldTypes);
        }

        @Override
        public void visit(PlanNodeVisitor visitor) {
            visitor.onOtherNode(this);
        }

        @Override
        protected void writeData0(ObjectDataOutput out) throws IOException {
            SerializationUtil.writeList(fieldTypes, out);
        }

        @Override
        protected void readData0(ObjectDataInput in) throws IOException {
            fieldTypes = SerializationUtil.readList(in);
        }
    }

    private static class TestUniInputPlanNode extends UniInputPlanNode {

        private List<QueryDataType> fieldTypes;

        private TestUniInputPlanNode() {
            // No-op.
        }

        private TestUniInputPlanNode(int id, TestZeroInputPlanNode upstream, List<QueryDataType> fieldTypes) {
            super(id, upstream);

            this.fieldTypes = fieldTypes;
        }

        @Override
        protected PlanNodeSchema getSchema0() {
            return new PlanNodeSchema(fieldTypes);
        }

        @Override
        public void visit0(PlanNodeVisitor visitor) {
            visitor.onOtherNode(this);
        }

        @Override
        protected void writeData1(ObjectDataOutput out) throws IOException {
            SerializationUtil.writeList(fieldTypes, out);
        }

        @Override
        protected void readData1(ObjectDataInput in) throws IOException {
            fieldTypes = SerializationUtil.readList(in);
        }
    }

    private static class TestPlanNodeVisitor extends TestPlanNodeVisitorAdapter {

        private final ArrayList<PlanNode> nodes = new ArrayList<>();

        @Override
        public void onRootNode(RootPlanNode node) {
            nodes.add(node);
        }

        @Override
        public void onOtherNode(PlanNode node) {
            nodes.add(node);
        }

        private ArrayList<PlanNode> getNodes() {
            return nodes;
        }
    }
}
