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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

public class SqlShowStatement extends SqlCall {

    public static final SqlSpecialOperator SHOW_MAPPINGS = new SqlSpecialOperator("SHOW EXTERNAL MAPPINGS", SqlKind.OTHER);
    public static final SqlSpecialOperator SHOW_VIEWS = new SqlSpecialOperator("SHOW VIEWS", SqlKind.OTHER);
    public static final SqlSpecialOperator SHOW_JOBS = new SqlSpecialOperator("SHOW JOBS", SqlKind.OTHER);

    private final ShowStatementTarget target;

    public SqlShowStatement(SqlParserPos pos, ShowStatementTarget target) {
        super(pos);
        this.target = target;
    }

    public ShowStatementTarget getTarget() {
        return target;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return target.operator;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(target.operator.getName());
    }

    /**
     * The argument of the SHOW command (e.g. SHOW MAPPINGS, SHOW VIEWS, SHOW JOBS).
     */
    public enum ShowStatementTarget {
        MAPPINGS(SHOW_MAPPINGS),
        VIEWS(SHOW_VIEWS),
        JOBS(SHOW_JOBS);

        private final SqlSpecialOperator operator;

        ShowStatementTarget(SqlSpecialOperator operator) {
            this.operator = operator;
        }
    }
}
