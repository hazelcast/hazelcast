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

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.validate.operators.special.HazelcastExplainOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * AST class for EXPLAIN PLAN FOR clause.
 */
public class SqlExplainStatement extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new HazelcastExplainOperator();

    private SqlNode explicandum;

    public SqlExplainStatement(SqlParserPos pos, SqlNode explicandum) {
        super(pos);
        this.explicandum = explicandum;
    }

    public SqlNode getExplicandum() {
        return explicandum;
    }

    /*
     * Exists for only special edge case: extract SqlSelect or any SqlSetOp statement from SqlOrderBy.
     */
    public void setExplicandum(SqlNode explicandum) {
        this.explicandum = explicandum;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(explicandum);
    }
}
