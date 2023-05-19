/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.jdbc.mysql;

import com.hazelcast.jet.sql.impl.validate.operators.string.HazelcastConcatOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

/**
 * Custom dialect for MySQL which allows correct unparsing of MySQL specific operators, like CONCAT
 */
public class HazelcastMySqlDialect extends MysqlSqlDialect {

    /**
     * Creates a HazelcastMySqlDialect.
     */
    public HazelcastMySqlDialect(Context context) {
        super(context);
    }

    @Override
    public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        switch (call.getKind()) {
            case OTHER:
                if (call.getOperator() instanceof HazelcastConcatOperator) {
                    unparseConcat(writer, call);
                    break;
                } else {
                    super.unparseCall(writer, call, leftPrec, rightPrec);
                    break;
                }

            default:
                super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }

    private void unparseConcat(SqlWriter writer, SqlCall call) {
        writer.print("CONCAT");
        Frame frame = writer.startList(FrameTypeEnum.PARENTHESES, "(", ")");
        for (SqlNode operand : call.getOperandList()) {
            writer.sep(",");
            operand.unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }
}
