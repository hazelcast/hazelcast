/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;

import java.util.Collections;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public class LogicalGetDdlRel extends SingleRel {

    public LogicalGetDdlRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input) {
        super(cluster, traits, input);
    }

    @Override
    protected RelDataType deriveRowType() {
        RelDataTypeFieldImpl ddlField = new RelDataTypeFieldImpl(
                "ddl",
                0,
                HazelcastTypeFactory.INSTANCE.createSqlType(VARCHAR));
        return new RelRecordType(Collections.singletonList(ddlField));
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalGetDdlRel(getCluster(), traitSet, sole(inputs));
    }
}
