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

package com.hazelcast.jet.sql.impl.parse;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.jet.sql.impl.parse.SqlShowStatement.ShowStatementTarget.RESOURCES;
import static com.hazelcast.jet.sql.impl.validate.ValidationUtil.isCatalogObjectNameValid;

public class SqlShowStatement extends SqlCall {

    public static final SqlSpecialOperator SHOW_MAPPINGS = new SqlSpecialOperator("SHOW EXTERNAL MAPPINGS", SqlKind.OTHER);
    public static final SqlSpecialOperator SHOW_VIEWS = new SqlSpecialOperator("SHOW VIEWS", SqlKind.OTHER);
    public static final SqlSpecialOperator SHOW_JOBS = new SqlSpecialOperator("SHOW JOBS", SqlKind.OTHER);
    public static final SqlSpecialOperator SHOW_TYPES = new SqlSpecialOperator("SHOW TYPES", SqlKind.OTHER);
    public static final SqlSpecialOperator SHOW_DATA_CONNECTIONS = new SqlSpecialOperator("SHOW DATA CONNECTIONS", SqlKind.OTHER);
    public static final SqlSpecialOperator SHOW_RESOURCES = new SqlSpecialOperator("SHOW RESOURCES FOR", SqlKind.OTHER);

    private final ShowStatementTarget target;
    private final SqlIdentifier dataConnectionName;

    public SqlShowStatement(SqlParserPos pos, @Nonnull ShowStatementTarget target, @Nullable SqlIdentifier dataConnectionName) {
        super(pos);
        this.target = target;
        this.dataConnectionName = dataConnectionName;
        assert dataConnectionName == null || target == RESOURCES;
    }

    public ShowStatementTarget getTarget() {
        return target;
    }

    /**
     * For target=RESOURCES returns the data connection name. Returns null, if:<ul>
     *     <li>target is other than RESOURCES
     *     <li>the schema isn't "hazelcast.public"
     * </ul>
     */
    @Nullable
    public String getDataConnectionNameWithoutSchema() {
        return dataConnectionName != null && isCatalogObjectNameValid(dataConnectionName)
                ? dataConnectionName.names.get(dataConnectionName.names.size() - 1)
                : null;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return target.operator;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        if (dataConnectionName != null) {
            return ImmutableNullableList.of(dataConnectionName);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(target.operator.getName());
        if (target.operator.equals(SHOW_RESOURCES) && dataConnectionName != null) {
            writer.keyword("FOR");
            dataConnectionName.unparse(writer, leftPrec, rightPrec);
        }
    }

    /**
     * The argument of the SHOW command (e.g. SHOW MAPPINGS, SHOW VIEWS, SHOW JOBS).
     */
    public enum ShowStatementTarget {
        MAPPINGS(SHOW_MAPPINGS),
        VIEWS(SHOW_VIEWS),
        JOBS(SHOW_JOBS),
        TYPES(SHOW_TYPES),
        DATACONNECTIONS(SHOW_DATA_CONNECTIONS),
        RESOURCES(SHOW_RESOURCES);

        private final SqlSpecialOperator operator;

        ShowStatementTarget(SqlSpecialOperator operator) {
            this.operator = operator;
        }
    }
}
