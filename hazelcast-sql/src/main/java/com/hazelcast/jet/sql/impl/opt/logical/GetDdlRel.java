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
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public class GetDdlRel extends AbstractRelNode {
    private final String namespace;
    private final String objectName;
    private final String schemaName;

    public GetDdlRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<String> operands) {
        super(cluster, traits);
        namespace = operands.get(0);
        objectName = operands.get(1);
        if (operands.size() > 2) {
            schemaName = operands.get(2);
        } else {
            schemaName = null;
        }
    }

    @Override
    protected RelDataType deriveRowType() {
        RelDataTypeFieldImpl ddlField = new RelDataTypeFieldImpl(
                "ddl",
                0,
                HazelcastTypeFactory.INSTANCE.createSqlType(VARCHAR));
        return new RelRecordType(Collections.singletonList(ddlField));
    }

    public List<String> operands() {
        return Arrays.asList(namespace, objectName, schemaName);
    }

    public String getNamespace() {
        return namespace;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("namespace", namespace)
                .item("object_name", objectName)
                .itemIf("schema_name", schemaName, schemaName != null);
    }
}
