package com.hazelcast.sql.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

public class HazelcastTableScanRel extends TableScan implements HazelcastRel {

    private final RelDataType rowType;

    public HazelcastTableScanRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        this(cluster, traitSet, table, table.getRowType());
    }

    public HazelcastTableScanRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RelDataType rowType) {
        super(cluster, traitSet, table);

        this.rowType = rowType;
    }

    @Override
    public RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public void visitForPlan(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }


}
