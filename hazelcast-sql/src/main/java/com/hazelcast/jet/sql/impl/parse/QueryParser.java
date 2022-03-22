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

package com.hazelcast.jet.sql.impl.parse;

import com.hazelcast.jet.sql.impl.CalciteConfiguration;
import com.hazelcast.jet.sql.impl.calcite.parser.HazelcastSqlParser;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlConformance;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.jet.sql.impl.validate.UnsupportedOperationVisitor;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.SqlErrorCode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.ParseException;

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

    public QueryParseResult parse(String sql) {
        try {
            return parse0(sql);
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

    private QueryParseResult parse0(String sql) throws SqlParseException {
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
