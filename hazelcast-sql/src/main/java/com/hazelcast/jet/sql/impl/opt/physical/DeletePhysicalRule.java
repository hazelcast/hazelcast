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

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.DeleteLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.JetConventions.PHYSICAL;

public final class DeletePhysicalRule extends ConverterRule {

    static final RelOptRule INSTANCE = new DeletePhysicalRule();

    private DeletePhysicalRule() {
        super(
                DeleteLogicalRel.class, LOGICAL, PHYSICAL,
                DeletePhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        DeleteLogicalRel logicalDelete = (DeleteLogicalRel) rel;

        return new DeletePhysicalRel(
                logicalDelete.getCluster(),
                OptUtils.toPhysicalConvention(logicalDelete.getTraitSet()),
                logicalDelete.getTable(),
                logicalDelete.getCatalogReader(),
                OptUtils.toPhysicalInput(logicalDelete.getInput()),
                logicalDelete.getOperation(),
                logicalDelete.getUpdateColumnList(),
                logicalDelete.getSourceExpressionList(),
                logicalDelete.isFlattened()
        );
    }
}
