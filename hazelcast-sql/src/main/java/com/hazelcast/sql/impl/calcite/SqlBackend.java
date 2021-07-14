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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.SqlToRelConverter.Config;

import java.util.List;

/**
 * Provides various customization points for the optimization engine.
 */
public interface SqlBackend {

    SqlParserImplFactory parserFactory();

    SqlValidator validator(
            CatalogReader catalogReader,
            HazelcastTypeFactory typeFactory,
            SqlConformance conformance,
            List<Object> arguments
    );

    SqlVisitor<Void> unsupportedOperationVisitor(
            CatalogReader catalogReader
    );

    SqlToRelConverter converter(
            RelOptTable.ViewExpander viewExpander,
            SqlValidator validator,
            Prepare.CatalogReader catalogReader,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable,
            Config config
    );

    SqlPlan createPlan(
            OptimizationTask task,
            QueryParseResult parseResult,
            OptimizerContext context
    );
}
