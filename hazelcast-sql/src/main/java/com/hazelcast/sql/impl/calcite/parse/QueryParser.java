/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.impl.ParseException;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformance;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Performs syntactic and semantic validation of the query.
 */
public class QueryParser {

    private final HazelcastTypeFactory typeFactory;
    private final CatalogReader catalogReader;
    private final SqlConformance conformance;
    private final List<Object> arguments;

    private final SqlBackend sqlBackend;
    private final SqlBackend jetSqlBackend;

    public QueryParser(
            HazelcastTypeFactory typeFactory,
            CatalogReader catalogReader,
            SqlConformance conformance,
            List<Object> arguments,
            @Nonnull SqlBackend sqlBackend,
            @Nonnull SqlBackend jetSqlBackend
    ) {
        this.typeFactory = typeFactory;
        this.catalogReader = catalogReader;
        this.conformance = conformance;
        this.arguments = arguments;

        this.sqlBackend = sqlBackend;
        this.jetSqlBackend = jetSqlBackend;
    }

    public QueryParseResult parse(String sql) {
        try {
            try {
                return parse(sql, sqlBackend);
            } catch (Exception e) {
                return parse(sql, jetSqlBackend);
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

    private QueryParseResult parse(String sql, SqlBackend sqlBackend) throws SqlParseException {
        assert sqlBackend != null;

        Config config = createConfig(sqlBackend.parserFactory());
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode topNode = parser.parseStmt();

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
