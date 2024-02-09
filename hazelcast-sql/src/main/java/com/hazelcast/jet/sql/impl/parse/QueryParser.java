/*
 * Copyright 2024 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.parse;

import com.hazelcast.jet.sql.impl.CalciteConfiguration;
import com.hazelcast.jet.sql.impl.calcite.parser.HazelcastSqlParser;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlConformance;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.jet.sql.impl.validate.UnsupportedOperationVisitor;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.security.NoOpSqlSecurityContext;
import com.hazelcast.sql.impl.security.SqlSecurityContext;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.ParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;

/**
 * Performs syntactic and semantic validation of the query.
 */
public class QueryParser {

    private static final SqlParser.Config CONFIG;

    static {
        SqlParser.ConfigBuilder configBuilder = SqlParser.configBuilder();
        CalciteConfiguration.DEFAULT.toParserConfig(configBuilder);
        configBuilder.setConformance(HazelcastSqlConformance.INSTANCE);
        configBuilder.setParserFactory(HazelcastSqlParser.FACTORY);
        CONFIG = configBuilder.build();
    }

    private final HazelcastSqlValidator validator;

    public QueryParser(HazelcastSqlValidator validator) {
        this.validator = validator;
    }

    //TODO: Is there any other way to not use visitor here?
    private static class TableNamesVisitor extends SqlBasicVisitor<Void> {
        private final Set<String> tableNames = new HashSet<>();

        @Override
        public Void visit(SqlIdentifier id) {
            tableNames.add(id.toString());
            return super.visit(id);
        }

        @Override
        public Void visit(SqlLiteral literal) {
            tableNames.add(literal.toString());
            return super.visit(literal);
        }

        public Set<String> getTableNames() {
            return tableNames;
        }

    }
    public static Set<String> getTablesFromSql(String sql) {
        SqlParser parser = SqlParser.create(sql, CONFIG);
        SqlNodeList statements = null;
        try {
            //TODO: Why SqlNode List works?
            statements = parser.parseStmtList();
        } catch (SqlParseException e) {
            return null;
        }
        Set<String> res = new HashSet<>();
        for (SqlNode statement : statements) {
            TableNamesVisitor visitor = new TableNamesVisitor();
            statement.accept(visitor);
            res.addAll(visitor.getTableNames());
        }
        return res;
    }

    public QueryParseResult parse(String sql, @Nonnull SqlSecurityContext ssc) {
        try {
            return parse0(sql, ssc);
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            String message;
            // Check particular type of exception which causes typical long multiline error messages.
            if (e instanceof SqlParseException && e.getCause() instanceof ParseException) {
                message = trimMessage(e.getMessage());
            } else {
                message = e.getMessage();
            }
            throw QueryException.error(SqlErrorCode.PARSING, message, e);
        }
    }

    // For test purposes only
    QueryParseResult parse(String sql) {
        return parse(sql, NoOpSqlSecurityContext.INSTANCE);
    }

    private QueryParseResult parse0(String sql, SqlSecurityContext sqlSecurityContext) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, CONFIG);
        SqlNodeList statements = parser.parseStmtList();
        if (statements.size() != 1) {
            throw QueryException.error(SqlErrorCode.PARSING, "The command must contain a single statement");
        }

        SqlNode topNode = statements.get(0);
        topNode.accept(new UnsupportedOperationVisitor(false));
        SqlNode node = validator.validate(topNode);
        node.accept(new UnsupportedOperationVisitor(true));

        return new QueryParseResult(
                node,
                new QueryParameterMetadata(validator.getParameterConverters(node))
        );
    }

    private static String trimMessage(String message) {
        String eol = System.getProperty("line.separator", "\n");
        String[] parts = message.split(eol, 2);
        return parts[0];
    }
}
