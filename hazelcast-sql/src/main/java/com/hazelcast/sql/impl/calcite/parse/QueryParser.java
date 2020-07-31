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
import com.hazelcast.sql.impl.JetSqlBackend;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlConformance;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.validate.SqlValidator;

import javax.annotation.Nullable;
import java.util.Set;

import static java.util.Collections.emptySet;

/**
 * Performs syntactic and semantic validation of the query, and converts the parse tree into a relational tree.
 */
public class QueryParser {

    private final SqlParser.Config parserConfig;

    private final SqlValidator validator;
    private final Set<SqlKind> extensionSqlKinds;
    private final Set<SqlOperator> extensionSqlOperators;

    @SuppressWarnings("unchecked")
    public QueryParser(
            SqlValidator validator,
            @Nullable JetSqlBackend jetSqlBackend
    ) {
        SqlParser.ConfigBuilder configBuilder = SqlParser.configBuilder();
        CasingConfiguration.DEFAULT.toParserConfig(configBuilder);
        configBuilder.setConformance(HazelcastSqlConformance.INSTANCE);
        if (jetSqlBackend != null) {
            configBuilder.setParserFactory((SqlParserImplFactory) jetSqlBackend.createParserFactory());
        }
        this.parserConfig = configBuilder.build();

        this.validator = validator;
        this.extensionSqlKinds = jetSqlBackend == null ? emptySet() : (Set<SqlKind>) jetSqlBackend.kinds();
        this.extensionSqlOperators = jetSqlBackend == null ? emptySet() : (Set<SqlOperator>) jetSqlBackend.operators();
    }

    public QueryParseResult parse(String sql) {
        try {
            SqlParser parser = SqlParser.create(sql, parserConfig);

            SqlNode node = validator.validate(parser.parseStmt());

            UnsupportedOperationVisitor visitor = new UnsupportedOperationVisitor(
                validator.getCatalogReader(),
                extensionSqlKinds,
                extensionSqlOperators
            );
            node.accept(visitor);

            return new QueryParseResult(
                node,
                validator.getParameterRowType(node),
                visitor.isExclusivelyImdgStatement()
            );

            // TODO: Get column names through SqlSelect.selectList[i].toString() (and, possibly, origins?)?
        } catch (Exception e) {
            throw QueryException.error(SqlErrorCode.PARSING, e.getMessage(), e);
        }
    }
}
