package com.hazelcast.sql.rules;

import com.hazelcast.internal.query.expression.ColumnExpression;
import com.hazelcast.internal.query.expression.ConstantExpression;
import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.internal.query.expression.ExtractorExpression;
import com.hazelcast.internal.query.plan.physical.MapScanPhysicalNode;
import com.hazelcast.internal.query.plan.physical.PhysicalNode;
import com.hazelcast.internal.query.plan.physical.PhysicalPlan;
import com.hazelcast.internal.query.plan.physical.ReceivePhysicalNode;
import com.hazelcast.internal.query.plan.physical.RootPhysicalNode;
import com.hazelcast.internal.query.plan.physical.SendPhysicalNode;
import com.hazelcast.internal.query.plan.physical.SortMergeReceivePhysicalNode;
import com.hazelcast.internal.query.plan.physical.SortPhysicalNode;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.ArrayList;
import java.util.List;

public class PhysicalPlanVisitor {

    private PhysicalPlan plan = new PhysicalPlan();
    private List<PhysicalNode> nodes = new ArrayList<>();
    private int currentEdgeId;

    public void visit(HazelcastRel rel) {
        // TODO: Add project
        // TODO: Add filter

        if (rel instanceof HazelcastRootRel)
            handleRoot((HazelcastRootRel)rel);
        else if (rel instanceof HazelcastTableScanRel)
            handleScan((HazelcastTableScanRel)rel);
        else if (rel instanceof HazelcastSortRel)
            handleSort((HazelcastSortRel)rel);
        else
            throw new UnsupportedOperationException("Unsupported: " + rel);
    }

    private void handleRoot(HazelcastRootRel root) {
        // TODO: In correct implementation we should always compare two adjacent nodes and decide whether new
        // TODO: fragment is needed.
        currentEdgeId++;

        PhysicalNode upstreamNode = nodes.remove(0);

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

            SortMergeReceivePhysicalNode receiveNode = new SortMergeReceivePhysicalNode(
                upstreamNode0.getExpressions(),
                upstreamNode0.getAscs(),
                1,
                currentEdgeId
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
                new ReceivePhysicalNode(currentEdgeId, 1)
            );

            plan.addNode(rootNode);
        }
    }

    private void handleScan(HazelcastTableScanRel scan) {
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
            null,       // Filter: // TODO: Need to implement FilterExec and merge rule!
            1           // Parallelism
        );

        nodes.add(scanNode);
    }

    private void handleSort(HazelcastSortRel sort) {
        assert nodes.size() == 1;

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

        SortPhysicalNode sortNode = new SortPhysicalNode(nodes.remove(0), expressions, ascs);

        nodes.add(sortNode);
    }

    public PhysicalPlan getPlan() {
        return plan;
    }
}
