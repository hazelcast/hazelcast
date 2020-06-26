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
import com.hazelcast.sql.impl.calcite.parser.HazelcastSqlParser;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlConformance;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * Performs syntactic and semantic validation of the query, and converts the parse tree into a relational tree.
 */
public class QueryParser {

    /** A hint to force execution of a query on Jet. */
    private static final String RUN_ON_JET_HINT = "jet";

    private static final SqlParser.Config CONFIG;
    private final SqlValidator validator;

    static {
        SqlParser.ConfigBuilder configBuilder = SqlParser.configBuilder();

        CasingConfiguration.DEFAULT.toParserConfig(configBuilder);
        configBuilder.setConformance(HazelcastSqlConformance.INSTANCE);
        configBuilder.setParserFactory(HazelcastSqlParser.FACTORY);

        CONFIG = configBuilder.build();
    }

    public QueryParser(SqlValidator validator) {
        this.validator = validator;
    }

    public QueryParseResult parse(String sql, boolean jetBackendPresent) {
        SqlNode node;
        RelDataType parameterRowType;

        boolean isImdg;
        try {
            SqlParser parser = SqlParser.create(sql, CONFIG);

            node = validator.validate(parser.parseStmt());

            UnsupportedOperationVisitor visitor = new UnsupportedOperationVisitor(validator.getCatalogReader());
            node.accept(visitor);

            if (!visitor.runsOnImdg() && !visitor.runsOnJet()) {
                // If there's a single feature that's not supported by either engine, the visitor already
                // threw an error when visiting. If we get here it means that there's some feature
                // missing in IMDG and a distinct feature missing in Jet.
                throw QueryException.error("The query contains an unsupported combination of features");
            }

            if (!visitor.runsOnImdg() && !jetBackendPresent) {
                throw QueryException.error("To run this query Hazelcast Jet must be on the classpath");
            }

            isImdg = visitor.runsOnImdg();
            // check for the Jet hint to force execution on Jet. Ignore the hint if query doesn't run on Jet
            if (isImdg && visitor.runsOnJet()) {
                boolean[] jetHinted = {false};
                node.accept(new SqlBasicVisitor<Void>() {
                    @Override
                    public Void visit(SqlCall node) {
                        jetHinted[0] |= node.getKind() == SqlKind.SELECT
                                && ((SqlSelect) node).getHints().getList().stream()
                                                     .anyMatch(n -> ((SqlHint) n).getName().equals(RUN_ON_JET_HINT));
                        return super.visit(node);
                    }
                });

                isImdg = !jetHinted[0];
            }

            parameterRowType = validator.getParameterRowType(node);

            // TODO: Get column names through SqlSelect.selectList[i].toString() (and, possibly, origins?)?
        } catch (Exception e) {
            throw QueryException.error(SqlErrorCode.PARSING, e.getMessage(), e);
        }

        return new QueryParseResult(node, parameterRowType, isImdg);
    }
}
