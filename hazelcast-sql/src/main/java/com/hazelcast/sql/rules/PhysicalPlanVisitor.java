package com.hazelcast.sql.rules;

import com.hazelcast.internal.query.expression.ConstantExpression;
import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.internal.query.expression.ExtractorExpression;
import com.hazelcast.internal.query.plan.physical.MapScanPhysicalNode;
import com.hazelcast.internal.query.plan.physical.PhysicalNode;
import com.hazelcast.internal.query.plan.physical.PhysicalPlan;
import com.hazelcast.internal.query.plan.physical.ReceivePhysicalNode;
import com.hazelcast.internal.query.plan.physical.RootPhysicalNode;
import com.hazelcast.internal.query.plan.physical.SendPhysicalNode;

import java.util.ArrayList;
import java.util.List;

public class PhysicalPlanVisitor {

    private PhysicalPlan plan;
    private final List<PhysicalNode> nodes = new ArrayList<>();
    private int currentEdgeId;

    public void visit(HazelcastRel rel) {
        if (rel instanceof HazelcastRootRel)
            handleRoot((HazelcastRootRel)rel);
        else if (rel instanceof HazelcastTableScanRel)
            handleScan((HazelcastTableScanRel)rel);
        else
            throw new UnsupportedOperationException("Unsupported: " + rel);
    }

    private void handleRoot(HazelcastRootRel root) {
        RootPhysicalNode rootNode = new RootPhysicalNode(
            new ReceivePhysicalNode(currentEdgeId)
        );

        nodes.add(rootNode);

        plan = new PhysicalPlan(nodes);
    }

    private void handleScan(HazelcastTableScanRel scan) {
        currentEdgeId++;

        // TODO: Handle schemas (in future)!
        String mapName = scan.getTable().getQualifiedName().get(0);

        // TODO: Supoprt expressions! Use REX visitor!
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

        // Send scan results to the edge 1.
        SendPhysicalNode sendNode = new SendPhysicalNode(
            currentEdgeId,               // Edge
            scanNode,                    // Underlying scan
            new ConstantExpression<>(1), // Partitioning info: REWORK!
            false                        // Partitioning info: REWORK!
        );

        nodes.add(sendNode);
    }

    public PhysicalPlan getPlan() {
        return plan;
    }
}
