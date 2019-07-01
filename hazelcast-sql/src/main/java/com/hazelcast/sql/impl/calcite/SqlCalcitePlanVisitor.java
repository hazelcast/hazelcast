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

import com.hazelcast.sql.impl.calcite.logical.rel.FilterLogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rel.ProjectLogicalRel;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExtractorExpression;
import com.hazelcast.sql.impl.physical.MapScanPhysicalNode;
import com.hazelcast.sql.impl.physical.PhysicalNode;
import com.hazelcast.sql.impl.PhysicalPlan;
import com.hazelcast.sql.impl.physical.ReceivePhysicalNode;
import com.hazelcast.sql.impl.physical.RootPhysicalNode;
import com.hazelcast.sql.impl.physical.SendPhysicalNode;
import com.hazelcast.sql.impl.physical.ReceiveSortMergePhysicalNode;
import com.hazelcast.sql.impl.physical.SortPhysicalNode;
import com.hazelcast.sql.impl.calcite.logical.rel.RootLogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rel.SortLogicalRel;
import com.hazelcast.sql.impl.calcite.logical.rel.MapScanLogicalRel;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.ArrayList;
import java.util.List;

/**
 * Plan visitor.
 */
public class SqlCalcitePlanVisitor {
    /** Prepared fragments. */
    private PhysicalPlan plan = new PhysicalPlan();

    /** Upstream nodes. Normally it is a one node, except of multi-source operations (e.g. joins, sets, subqueries). */
    private List<PhysicalNode> upstreamNodes = new ArrayList<>();

    /** Current edge ID. */
    private int currentEdgeId;

    /**
     * Visit root node.
     *
     * @param root Root node.
     */
    public void visitRoot(RootLogicalRel root) {
        // TODO: In correct implementation we should always compare two adjacent nodes and decide whether new
        // TODO: fragment is needed.
        currentEdgeId++;

        PhysicalNode upstreamNode = this.upstreamNodes.remove(0);

        if (upstreamNode instanceof SortPhysicalNode) {
            SortPhysicalNode upstreamNode0 = (SortPhysicalNode)upstreamNode;

            // Sort-merge receiver.
            SendPhysicalNode sendNode = new SendPhysicalNode(
                currentEdgeId,               // Edge
                upstreamNode,                // Underlying node
                new ConstantExpression<>(1), // Partitioning info: REWORK!
                false                        // Partitioning info: REWORK!
            );

            plan.addNode(sendNode);

            ReceiveSortMergePhysicalNode receiveNode = new ReceiveSortMergePhysicalNode(
                currentEdgeId,
                upstreamNode0.getExpressions(),
                upstreamNode0.getAscs()
            );

            RootPhysicalNode rootNode = new RootPhysicalNode(
                receiveNode
            );

            plan.addNode(rootNode);
        }
        else {
            // Normal receiver.
            SendPhysicalNode sendNode = new SendPhysicalNode(
                currentEdgeId,               // Edge
                upstreamNode,                // Underlying node
                new ConstantExpression<>(1), // Partitioning info: REWORK!
                false                        // Partitioning info: REWORK!
            );

            plan.addNode(sendNode);

            RootPhysicalNode rootNode = new RootPhysicalNode(
                new ReceivePhysicalNode(currentEdgeId)
            );

            plan.addNode(rootNode);
        }
    }

    /**
     * Visit table scan.
     *
     * @param scan Table scan.
     */
    public void visitTableScan(MapScanLogicalRel scan) {
        // TODO: Handle schemas (in future)!
        String mapName = scan.getTable().getQualifiedName().get(0);

        // TODO: Supoprt expressions? (use REX visitor)
        List<String> fieldNames = scan.getRowType().getFieldNames();

        List<Expression> projection = new ArrayList<>();

        for (String fieldName : fieldNames)
            projection.add(new ExtractorExpression(fieldName));

        MapScanPhysicalNode scanNode = new MapScanPhysicalNode(
            mapName, // Scan
            projection, // Project
            null       // Filter: // TODO: Need to implement FilterExec and merge rule!
        );

        upstreamNodes.add(scanNode);
    }

    /**
     * Visit project.
     *
     * @param project Project.
     */
    public void visitProject(ProjectLogicalRel project) {
        // TODO: Implement.
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Visit filter.
     *
     * @param filter Filter.
     */
    public void visitFilter(FilterLogicalRel filter) {
        // TODO: Implement.
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Visit sort.
     *
     * @param sort Sort.
     */
    public void visitSort(SortLogicalRel sort) {
        assert upstreamNodes.size() == 1;

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

        SortPhysicalNode sortNode = new SortPhysicalNode(upstreamNodes.remove(0), expressions, ascs);

        upstreamNodes.add(sortNode);
    }

    public PhysicalPlan getPlan() {
        return plan;
    }
}
