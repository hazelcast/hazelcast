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

import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.SqlBackend;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlConformance;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Performs syntactic and semantic validation of the query.
 */
public class QueryParser {

    private final HazelcastTypeFactory typeFactory;
    private final CatalogReader catalogReader;
    private final SqlConformance conformance;

    private final SqlBackend sqlBackend;
    private final SqlBackend jetSqlBackend;

    public QueryParser(
            HazelcastTypeFactory typeFactory,
            CatalogReader catalogReader,
            SqlConformance conformance,
            @Nonnull SqlBackend sqlBackend,
            @Nullable SqlBackend jetSqlBackend
    ) {
        this.typeFactory = typeFactory;
        this.catalogReader = catalogReader;
        this.conformance = conformance;

        this.sqlBackend = sqlBackend;
        this.jetSqlBackend = jetSqlBackend;
    }

    public QueryParseResult parse(String sql) {
        try {
            try {
                return parse(sql, sqlBackend);
            } catch (Exception e) {
                if (jetSqlBackend != null) {
                    return parse(sql, jetSqlBackend);
                } else {
                    throw e;
                }
            }
        } catch (Exception e) {
            throw QueryException.error(SqlErrorCode.PARSING, e.getMessage(), e);
        }
    }

    private QueryParseResult parse(String sql, SqlBackend sqlBackend) throws SqlParseException {
        assert sqlBackend != null;

        Config config = createConfig(sqlBackend.parserFactory());
        SqlParser parser = SqlParser.create(sql, config);

        SqlValidator validator = sqlBackend.validator(catalogReader, typeFactory, conformance);
        SqlNode node = validator.validate(parser.parseStmt());

        SqlVisitor<Void> visitor = sqlBackend.unsupportedOperationVisitor(catalogReader);
        node.accept(visitor);

        return new QueryParseResult(
            node,
            validator.getParameterRowType(node),
            validator,
            sqlBackend
        );
    }

    private static Config createConfig(SqlParserImplFactory parserImplFactory) {
        SqlParser.ConfigBuilder configBuilder = SqlParser.configBuilder();
        CasingConfiguration.DEFAULT.toParserConfig(configBuilder);
        configBuilder.setConformance(HazelcastSqlConformance.INSTANCE);
        if (parserImplFactory != null) {
            configBuilder.setParserFactory(parserImplFactory);
        }
        return configBuilder.build();
    }
}
