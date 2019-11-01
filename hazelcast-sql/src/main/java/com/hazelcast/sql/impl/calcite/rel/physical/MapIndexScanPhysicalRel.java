
package com.hazelcast.sql.impl.calcite.rel.physical;

import com.hazelcast.sql.impl.calcite.schema.HazelcastTableIndex;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Map index scan operator.
 */
public class MapIndexScanPhysicalRel extends AbstractMapScanPhysicalRel {
    /** Target index. */
    private final HazelcastTableIndex index;

    /** Disjunctive set of filters to be applied to index. */
    private final List<RexNode> indexFilters;

    public MapIndexScanPhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<Integer> projects,
        RexNode filter,
        HazelcastTableIndex index,
        List<RexNode> indexFilters
    ) {
        super(cluster, traitSet, table, projects, filter);

        this.index = index;
        this.indexFilters = indexFilters;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MapIndexScanPhysicalRel(getCluster(), traitSet, getTable(), projects, filter, index, indexFilters);
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        visitor.onMapIndexScan(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
           .item("index", index)
           .item("indexFilters", indexFilters);
    }
}
