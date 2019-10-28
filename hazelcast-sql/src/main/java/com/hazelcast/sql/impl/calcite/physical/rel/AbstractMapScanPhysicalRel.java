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

package com.hazelcast.sql.impl.calcite.physical.rel;

import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Base class for physical map scans.
 */
public abstract class AbstractMapScanPhysicalRel extends TableScan implements PhysicalRel {
    /** Projection. */
    protected final List<Integer> projects;

    /** Filter. */
    protected final RexNode filter;

    public AbstractMapScanPhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<Integer> projects,
        RexNode filter
    ) {
        super(cluster, traitSet, table);

        this.projects = projects;
        this.filter = filter;
    }

    @Override
    public RelDataType deriveRowType() {
        RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        List<RelDataTypeField> fieldList = table.getRowType().getFieldList();

        for (int project : projects) {
            builder.add(fieldList.get(project));
        }

        return builder.build();
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("projects", projects, !projects.isEmpty())
            .itemIf("filter", filter, filter != null);
    }

    public List<Integer> getProjects() {
        return projects;
    }

    public RexNode getFilter() {
        return filter;
    }

    /**
     * @return Name of the map.
     */
    public String getMapName() {
        return table.unwrap(HazelcastTable.class).getName();
    }
}
