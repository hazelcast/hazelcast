/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.calcite.opt.SqlOptimizer;
import com.hazelcast.sql.impl.calcite.parser.SqlCreateTable;
import com.hazelcast.sql.impl.calcite.parser.SqlDropTable;
import com.hazelcast.sql.impl.calcite.parser.SqlOption;
import com.hazelcast.sql.impl.parser.CreateTableStatement;
import com.hazelcast.sql.impl.parser.DqlStatement;
import com.hazelcast.sql.impl.parser.DropTableStatement;
import com.hazelcast.sql.impl.parser.SqlParseTask;
import com.hazelcast.sql.impl.parser.SqlParser;
import com.hazelcast.sql.impl.parser.Statement;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.schema.Catalog;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.TableSchema;
import com.hazelcast.sql.impl.schema.TableSchema.Field;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTableResolver;
import com.hazelcast.sql.impl.schema.map.ReplicatedMapTableResolver;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Calcite-based SQL parser.
 */
public class CalciteSqlParser implements SqlParser {

    private final NodeEngine nodeEngine;
    private final SqlOptimizer optimizer;

    public CalciteSqlParser(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.optimizer = new SqlOptimizer(nodeEngine);
    }

    @Override
    public Statement parse(SqlParseTask task) {
        // 1. Prepare context.
        ExecutionContext context = createContext(task.getCatalog(), task.getSearchPaths());

        // 2. Parse SQL statement.
        SqlNode node = context.parse(task.getSql());

        // 3. Convert to an operation.
        if (node instanceof SqlCreateTable) {
            return toCreateTableStatement((SqlCreateTable) node);
        } else if (node instanceof SqlDropTable) {
            return toDropTableStatement((SqlDropTable) node);
        } else {
            SqlNode validated = context.validate(node);
            Plan plan = optimizer.optimize(task.getSql(), validated, context);
            return new DqlStatement(plan);
        }
    }

    private ExecutionContext createContext(Catalog catalog, List<List<String>> searchPaths) {
        List<TableResolver> tableResolvers = asList(
                catalog,
                new PartitionedMapTableResolver(nodeEngine),
                new ReplicatedMapTableResolver(nodeEngine)
        );

        int memberCount = nodeEngine.getClusterService().getSize(MemberSelectors.DATA_MEMBER_SELECTOR);

        return ExecutionContext.create(
                tableResolvers,
                searchPaths,
                memberCount
        );
    }

    private Statement toCreateTableStatement(SqlCreateTable sqlCreateTable) {
        List<Field> fields = sqlCreateTable.columns()
                                           .map(column -> new Field(column.name(), column.type().type()))
                                           .collect(toList());
        Map<String, String> options = sqlCreateTable.options()
                                                    .collect(toMap(SqlOption::key, SqlOption::value));
        TableSchema schema = new TableSchema(sqlCreateTable.name(), sqlCreateTable.type(), fields, options);

        return new CreateTableStatement(
                schema,
                sqlCreateTable.getReplace(),
                sqlCreateTable.ifNotExists()
        );
    }

    private Statement toDropTableStatement(SqlDropTable sqlDropTable) {
        return new DropTableStatement(
                sqlDropTable.name(),
                sqlDropTable.ifExists()
        );
    }
}
