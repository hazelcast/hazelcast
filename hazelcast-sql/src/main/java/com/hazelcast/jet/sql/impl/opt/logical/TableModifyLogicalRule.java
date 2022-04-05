/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

public final class TableModifyLogicalRule extends ConverterRule {

    static final RelOptRule INSTANCE = new TableModifyLogicalRule();

    private TableModifyLogicalRule() {
        super(
                LogicalTableModify.class, Convention.NONE, LOGICAL,
                TableModifyLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        TableModify tableModify = (LogicalTableModify) rel;
        RelNode input = tableModify.getInput();

        return new TableModifyLogicalRel(
                tableModify.getCluster(),
                OptUtils.toLogicalConvention(tableModify.getTraitSet()),
                tableModify.getTable(),
                tableModify.getCatalogReader(),
                OptUtils.toLogicalInput(input),
                tableModify.getOperation(),
                tableModify.getUpdateColumnList(),
                tableModify.getSourceExpressionList(),
                tableModify.isFlattened()
        );
    }
}
