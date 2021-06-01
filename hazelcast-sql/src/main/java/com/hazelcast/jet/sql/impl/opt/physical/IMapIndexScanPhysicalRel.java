/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.opt.AbstractScanRel;
import com.hazelcast.sql.impl.calcite.opt.cost.CostUtils;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.toList;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.calcite.rel.RelFieldCollation.Direction;

/**
 * Map index scan operator.
 */
public class IMapIndexScanPhysicalRel extends AbstractScanRel implements PhysicalRel {

    private final MapTableIndex index;
    private final IndexFilter indexFilter;
    private final List<QueryDataType> converterTypes;
    private final RexNode indexExp;
    private final RexNode remainderExp;

    public IMapIndexScanPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            MapTableIndex index,
            IndexFilter indexFilter,
            List<QueryDataType> converterTypes,
            RexNode indexExp,
            RexNode remainderExp
    ) {
        super(cluster, traitSet, table);

        this.index = index;
        this.indexFilter = indexFilter;
        this.converterTypes = converterTypes;
        this.indexExp = indexExp;
        this.remainderExp = remainderExp;
    }

    public Expression<Boolean> filter(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema schema = OptUtils.schema(getTable());
        return filter(schema, remainderExp, parameterMetadata);
    }

    public List<Expression<?>> projection(QueryParameterMetadata parameterMetadata) {
        PlanNodeSchema schema = OptUtils.schema(getTable());

        HazelcastTable table = getTable().unwrap(HazelcastTable.class);

        List<Integer> projects = table.getProjects();
        List<RexNode> projection = new ArrayList<>(projects.size());
        for (Integer index : projects) {
            TableField field = table.getTarget().getField(index);
            RelDataType relDataType = OptUtils.convert(field, getCluster().getTypeFactory());
            projection.add(new RexInputRef(index, relDataType));
        }

        return project(schema, projection, parameterMetadata);
    }

    public MapTableIndex getIndex() {
        return index;
    }

    public IndexFilter getIndexFilter() {
        return indexFilter;
    }

    public List<QueryDataType> getConverterTypes() {
        return converterTypes;
    }

    public List<Boolean> getAscs() {
        RelCollation collation = getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        assert collation != null;
        int size = collation.getFieldCollations().size();

        List<Boolean> ascs = new ArrayList<>(size);
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
            Boolean asc = fieldCollation.getDirection() == Direction.ASCENDING ? TRUE : FALSE;
            ascs.add(asc);
        }
        return ascs;
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        List<QueryDataType> fieldTypes = toList(projection(parameterMetadata), Expression::getType);
        return new PlanNodeSchema(fieldTypes);
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onIndexScan(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IMapIndexScanPhysicalRel(
                getCluster(),
                traitSet,
                getTable(),
                index,
                indexFilter,
                converterTypes,
                indexExp,
                remainderExp
        );
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("index", index.getName())
                .item("indexExp", indexExp)
                .item("remainderExp", remainderExp);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double rowCount = table.getRowCount();

        if (indexExp != null) {
            rowCount = CostUtils.adjustFilteredRowCount(rowCount, RelMdUtil.guessSelectivity(indexExp));
        }

        if (remainderExp != null) {
            rowCount = CostUtils.adjustFilteredRowCount(rowCount, RelMdUtil.guessSelectivity(remainderExp));
        }

        return rowCount;
    }
}
