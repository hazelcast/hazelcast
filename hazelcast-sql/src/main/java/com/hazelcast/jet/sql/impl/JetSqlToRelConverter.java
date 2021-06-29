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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.sql.impl.parse.SqlExtendedInsert;
import com.hazelcast.sql.impl.calcite.HazelcastSqlToRelConverter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;

public class JetSqlToRelConverter extends HazelcastSqlToRelConverter {

    public JetSqlToRelConverter(
            RelOptTable.ViewExpander viewExpander,
            SqlValidator validator,
            CatalogReader catalogReader,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable,
            Config config
    ) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    }

    @Override
    protected RelNode convertInsert(SqlInsert insert0) {
        SqlExtendedInsert insert = (SqlExtendedInsert) insert0;
        TableModify modify = (TableModify) super.convertInsert(insert);
        return insert.isInsert() ? new LogicalTableInsert(modify) : new LogicalTableSink(modify);
    }
}
