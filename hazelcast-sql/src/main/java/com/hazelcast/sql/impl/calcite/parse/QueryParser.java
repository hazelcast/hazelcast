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

package com.hazelcast.sql.impl.calcite.parse;

import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlConformance;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * Performs syntactic and semantic validation of the query, and converts the parse tree into a relational tree.
 */
public class QueryParser {

    private static final SqlParser.Config CONFIG;
    private final SqlValidator validator;

    static {
        SqlParser.ConfigBuilder configBuilder = SqlParser.configBuilder();

        CasingConfiguration.DEFAULT.toParserConfig(configBuilder);
        configBuilder.setConformance(HazelcastSqlConformance.INSTANCE);

        CONFIG = configBuilder.build();
    }

    public QueryParser(SqlValidator validator) {
        this.validator = validator;
    }

    public QueryParseResult parse(String sql) {
        SqlNode node;
        RelDataType parameterRowType;

        try {
            SqlParser parser = SqlParser.create(sql, CONFIG);

            node = validator.validate(parser.parseStmt());

            node.accept(UnsupportedOperationVisitor.INSTANCE);

            parameterRowType = validator.getParameterRowType(node);
        } catch (Exception e) {
            throw QueryException.error(SqlErrorCode.PARSING, e.getMessage(), e);
        }

        return new QueryParseResult(node, parameterRowType);
    }
}
