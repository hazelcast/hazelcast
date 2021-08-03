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

package com.hazelcast.sql.impl.calcite.parse;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.calcite.CalciteConfiguration;
import com.hazelcast.sql.impl.calcite.SqlBackend;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlConformance;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.impl.ParseException;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformance;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Performs syntactic and semantic validation of the query.
 */
public class QueryParser {

    private final HazelcastTypeFactory typeFactory;
    private final CatalogReader catalogReader;
    private final SqlConformance jetConformance;
    private final List<Object> arguments;

    private final SqlBackend jetSqlBackend;

    public QueryParser(
            HazelcastTypeFactory typeFactory,
            CatalogReader catalogReader,
            SqlConformance jetConformance,
            List<Object> arguments,
            @Nullable SqlBackend jetSqlBackend
    ) {
        this.typeFactory = typeFactory;
        this.catalogReader = catalogReader;
        this.jetConformance = jetConformance;
        this.arguments = arguments;
        this.jetSqlBackend = jetSqlBackend;
    }

    public QueryParseResult parse(String sql) {
        try {
            if (jetSqlBackend != null) {
                return parse(sql, jetSqlBackend, jetConformance);
            } else {
                // TODO: move this check to SqlServiceImpl.
                throw new RuntimeException("Jet is disabled, see JetConfig#setEnabled.");
            }
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

    private QueryParseResult parse(String sql, SqlBackend sqlBackend, SqlConformance conformance) throws SqlParseException {
        Config config = createConfig(sqlBackend.parserFactory());
        SqlParser parser = SqlParser.create(sql, config);
        SqlNodeList statements = parser.parseStmtList();
        if (statements.size() != 1) {
            throw QueryException.error(SqlErrorCode.PARSING, "The command must contain a single statement");
        }
        SqlNode topNode = statements.get(0);

        HazelcastSqlValidator validator =
                (HazelcastSqlValidator) sqlBackend.validator(catalogReader, typeFactory, conformance, arguments);
        SqlNode node = validator.validate(topNode);

        SqlVisitor<Void> visitor = sqlBackend.unsupportedOperationVisitor(catalogReader);
        node.accept(visitor);

        return new QueryParseResult(
                node,
                new QueryParameterMetadata(validator.getParameterConverters(node)),
                validator,
                sqlBackend,
                validator.isInfiniteRows()
        );
    }

    private static Config createConfig(SqlParserImplFactory parserImplFactory) {
        SqlParser.ConfigBuilder configBuilder = SqlParser.configBuilder();
        CalciteConfiguration.DEFAULT.toParserConfig(configBuilder);
        configBuilder.setConformance(HazelcastSqlConformance.INSTANCE);
        if (parserImplFactory != null) {
            configBuilder.setParserFactory(parserImplFactory);
        }
        return configBuilder.build();
    }

    private static String trimMessage(String message) {
        String eol = System.getProperty("line.separator", "\n");
        String[] parts = message.split(eol, 2);
        return parts[0];
    }
}
