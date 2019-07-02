/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.impl.QueryFragment;
import com.hazelcast.sql.impl.calcite.physical.rel.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.PhysicalRelVisitor;
import com.hazelcast.sql.impl.calcite.physical.rel.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.SingletonExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.SortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.SortPhysicalRel;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExtractorExpression;
import com.hazelcast.sql.impl.physical.MapScanPhysicalNode;
import com.hazelcast.sql.impl.physical.PhysicalNode;
import com.hazelcast.sql.impl.physical.ReceivePhysicalNode;
import com.hazelcast.sql.impl.physical.ReceiveSortMergePhysicalNode;
import com.hazelcast.sql.impl.physical.RootPhysicalNode;
import com.hazelcast.sql.impl.physical.SendPhysicalNode;
import com.hazelcast.sql.impl.physical.SortPhysicalNode;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Visitor which produces query plan.
 */
public class PlanCreateVisitor implements PhysicalRelVisitor {
    /** Members with partitions. */
    private final Set<String> partMemberIds;

    /** Local member ID. */
    private final String localMemberId;

    /** Prepared fragments. */
    private final List<QueryFragment> fragments = new ArrayList<>();

    /** Upstream nodes. Normally it is a one node, except of multi-source operations (e.g. joins, sets, subqueries). */
    private final Deque<PhysicalNode> upstreamNodes = new ArrayDeque<>();

    /** ID of current edge. */
    private int currentEdge;

    /** Current outbound edges. */
    private Integer currentOutboundEdge;

    /** Current inbound edges. */
    private List<Integer> currentInboundEdges;

    public PlanCreateVisitor(Set<String> partMemberIds, String localMemberId) {
        this.partMemberIds = partMemberIds;
        this.localMemberId = localMemberId;
    }

    public List<QueryFragment> getFragments() {
        return fragments;
    }

    @Override
    public void onRoot(RootPhysicalRel root) {
        PhysicalNode upstreamNode = pollSingleUpstream();

        RootPhysicalNode rootNode = new RootPhysicalNode(
            upstreamNode
        );

        Set<String> memberIds = new HashSet<>();

        memberIds.add(localMemberId);

        addFragment(rootNode, memberIds);
    }

    @Override
    public void onMapScan(MapScanPhysicalRel rel) {
        // TODO: Handle schemas (in future)!
        String mapName = rel.getTable().getQualifiedName().get(0);

        // TODO: Support expressions (use REX visitor)
        List<String> fieldNames = rel.getRowType().getFieldNames();

        List<Expression> projection = new ArrayList<>();

        for (String fieldName : fieldNames)
            projection.add(new ExtractorExpression(fieldName));

        MapScanPhysicalNode mapScanNode = new MapScanPhysicalNode(
            mapName,    // Scan
            projection, // Project
            null        // Filter: // TODO: Need to implement FilterExec and merge rule!
        );

        pushUpstream(mapScanNode);
    }

    @Override
    public void onSort(SortPhysicalRel sort) {
        PhysicalNode upstreamNode = pollSingleUpstream();

        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();

        List<Expression> expressions = new ArrayList<>(collations.size());
        List<Boolean> ascs = new ArrayList<>(collations.size());

        for (RelFieldCollation collation : collations) {
            // TODO: Proper direction handling (see all enum values).
            RelFieldCollation.Direction direction = collation.getDirection();
            int idx = collation.getFieldIndex();

            // TODO: Use fieldExps here.
            expressions.add(new ColumnExpression(idx));
            ascs.add(!direction.isDescending());
        }

        SortPhysicalNode sortNode = new SortPhysicalNode(upstreamNode, expressions, ascs);

        pushUpstream(sortNode);
    }

    @Override
    public void onSingletonExchange(SingletonExchangePhysicalRel rel) {
        // Get upstream node.
        PhysicalNode upstreamNode = pollSingleUpstream();

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        addOutboundEdge(edge);

        SendPhysicalNode sendNode = new SendPhysicalNode(
            edge,                        // Edge
            upstreamNode,                // Underlying node
            new ConstantExpression<>(1), // Partitioning info: REWORK!
            false                        // Partitioning info: REWORK!
        );

        addFragment(sendNode, partMemberIds);

        // Create receiver.
        addInboundEdge(edge);

        ReceivePhysicalNode receiveNode = new ReceivePhysicalNode(edge);

        pushUpstream(receiveNode);
    }

    @Override
    public void onSortMergeExchange(SortMergeExchangePhysicalRel rel) {
        // Get upstream node. It should be sort node.
        PhysicalNode upstreamNode = pollSingleUpstream();

        assert upstreamNode instanceof SortPhysicalNode;

        SortPhysicalNode sortNode = (SortPhysicalNode)upstreamNode;

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        addOutboundEdge(edge);

        SendPhysicalNode sendNode = new SendPhysicalNode(
            edge,                        // Edge
            sortNode,                    // Underlying node
            new ConstantExpression<>(1), // Partitioning info: REWORK!
            false                        // Partitioning info: REWORK!
        );

        addFragment(sendNode, partMemberIds);

        // Create a receiver and push it to stack.
        addInboundEdge(edge);

        ReceiveSortMergePhysicalNode receiveNode = new ReceiveSortMergePhysicalNode(
            edge,
            sortNode.getExpressions(),
            sortNode.getAscs()
        );

        pushUpstream(receiveNode);
    }

    /**
     * Push node to upstream stack.
     *
     * @param node Node.
     */
    private void pushUpstream(PhysicalNode node) {
        upstreamNodes.add(node);
    }

    /**
     * Poll an upstream node which is expected to be the only available in the stack.
     *
     * @return Upstream node.
     */
    private PhysicalNode pollSingleUpstream() {
        assert upstreamNodes.size() == 1;

        return upstreamNodes.pollFirst();
    }

    /**
     * Create new fragment and clear intermediate state.
     *
     * @param node Node.
     * @param memberIds Member IDs.
     */
    private void addFragment(PhysicalNode node, Set<String> memberIds) {
        assert upstreamNodes.isEmpty();

        QueryFragment fragment = new QueryFragment(node, currentOutboundEdge, currentInboundEdges, memberIds, 1);

        currentOutboundEdge = null;
        currentInboundEdges = null;

        fragments.add(fragment);
    }

    private int nextEdge() {
        return currentEdge++;
    }

    private void addInboundEdge(int edgeId) {
        if (currentInboundEdges == null)
            currentInboundEdges = new ArrayList<>(1);

        currentInboundEdges.add(edgeId);
    }

    private void addOutboundEdge(int edgeId) {
        assert currentOutboundEdge == null;

        currentOutboundEdge = edgeId;
    }
}
