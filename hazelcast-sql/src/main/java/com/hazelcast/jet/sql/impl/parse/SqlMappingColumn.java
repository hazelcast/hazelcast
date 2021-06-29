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

import com.hazelcast.jet.sql.impl.EventTimePolicySupplier;
import com.hazelcast.jet.sql.impl.EventTimePolicySupplier.LagEventTimePolicySupplier;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.List;

import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;
import static java.util.Objects.requireNonNull;

public class SqlMappingColumn extends SqlCall {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("COLUMN DECLARATION", SqlKind.COLUMN_DECL);

    private final SqlIdentifier name;
    private final SqlDataType type;
    private final SqlIdentifier externalName;
    private final SqlLiteral limitingLag;

    public SqlMappingColumn(
            SqlIdentifier name,
            SqlDataType type,
            SqlIdentifier externalName,
            SqlLiteral limitingLag,
            SqlParserPos pos
    ) {
        super(pos);

        this.name = requireNonNull(name, "Column name should not be null");
        this.type = requireNonNull(type, "Column type should not be null");
        this.externalName = externalName;
        this.limitingLag = limitingLag;
    }

    public String name() {
        return name.getSimple();
    }

    public QueryDataType type() {
        return type.type();
    }

    public String externalName() {
        return externalName == null ? null : externalName.toString();
    }

    public boolean isSourceOfEventTimestamps() {
        return limitingLag != null;
    }

    public EventTimePolicySupplier eventTimePolicySupplier() {
        if (limitingLag == null) {
            return null;
        } else {
            assert limitingLag.getTypeName().getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME;

            return new LagEventTimePolicySupplier(name(), limitingLag.getValueAs(BigDecimal.class).longValueExact());
        }
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, type, externalName, limitingLag);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        name.unparse(writer, leftPrec, rightPrec);
        if (type != null) {
            type.unparse(writer, leftPrec, rightPrec);
        }
        if (externalName != null) {
            writer.keyword("EXTERNAL NAME");
            externalName.unparse(writer, leftPrec, rightPrec);
        }
        if (limitingLag != null) {
            writer.keyword("WATERMARK");
            writer.keyword("LAG");
            writer.print("(");
            limitingLag.unparse(writer, leftPrec, rightPrec);
            writer.print(")");
        }
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (isSourceOfEventTimestamps()) {
            if (type() != QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME) {
                throw validator.newValidationError(type, RESOURCE.invalidWatermarkColumnType());
            }
            if (limitingLag.getTypeName().getFamily() != SqlTypeFamily.INTERVAL_DAY_TIME) {
                throw validator.newValidationError(type, RESOURCE.invalidLagInterval(limitingLag.getTypeName()));
            }
            if (limitingLag.getValueAs(BigDecimal.class).longValueExact() < 0) {
                throw validator.newValidationError(type, RESOURCE.negativeLag());
            }
        }
    }
}

